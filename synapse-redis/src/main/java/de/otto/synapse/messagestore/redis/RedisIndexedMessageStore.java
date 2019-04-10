package de.otto.synapse.messagestore.redis;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.Index;
import de.otto.synapse.messagestore.Indexer;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreEntry;
import de.otto.synapse.translator.*;
import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.DefaultHeaderAttr.MSG_ID;
import static java.util.Arrays.asList;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Redis-based implementation of a WritableMessageStore.
 *
 * <p>
 *     The store can be configured like a ring-buffer to only store the latest N messages.
 * </p>
 */
@Beta
public class RedisIndexedMessageStore implements MessageStore {

    private static final Logger LOG = getLogger(RedisIndexedMessageStore.class);
    private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE;

    private final String name;
    private final Indexer indexer;
    private final RedisTemplate<String, String> redisTemplate;
    private final int batchSize;
    private final int maxSize;
    private final Encoder<String> encoder;
    private final Decoder<String> decoder;
    private final long maxAge;

    /**
     * @param name the name of the message store
     * @param batchSize the size of the batches used to fetch messages from Redis
     * @param maxMessages the maximum number of messages stored in the message store
     * @param maxAge maximum number of seconds after that a message will be evicted
     * @param indexer the {@code Indexer} used to index entities stored in the message store
     * @param stringRedisTemplate the RedisTemplate used to access Redis
     */
    public RedisIndexedMessageStore(final String name,
                                    final int batchSize,
                                    final int maxMessages,
                                    final long maxAge,
                                    final Indexer indexer,
                                    final RedisTemplate<String, String> stringRedisTemplate) {
        this(name, batchSize, maxMessages, maxAge, indexer, stringRedisTemplate, new TextEncoder(MessageFormat.V2), new TextDecoder());
    }

    /**
     * @param name the name of the message store
     * @param batchSize the size of the batches used to fetch messages from Redis
     * @param maxMessages the maximum number of messages stored in the message store
     * @param maxAge maximum number of seconds after that a message will be evicted
     * @param indexer the {@code Indexer} used to index entities stored in the message store
     * @param stringRedisTemplate the RedisTemplate used to access Redis
     * @param messageEncoder the encoder used to encode messages into the string-representation stored in Redis
     * @param messageDecoder the decoder used to decode messages from the string-representation stored in Redis
     */
    public RedisIndexedMessageStore(final String name,
                                    final int batchSize,
                                    final int maxMessages,
                                    final long maxAge,
                                    final Indexer indexer,
                                    final RedisTemplate<String, String> stringRedisTemplate,
                                    final Encoder<String> messageEncoder,
                                    final Decoder<String> messageDecoder) {
        this.name = name;
        this.maxAge = maxAge;
        this.indexer = indexer;
        this.redisTemplate = stringRedisTemplate;
        this.batchSize = batchSize;
        this.maxSize = maxMessages;
        this.encoder = messageEncoder;
        this.decoder = messageDecoder;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(final MessageStoreEntry entry) {
        final MessageStoreEntry indexedEntry = indexer.index(entry);
        final TextMessage textMessage = indexedEntry.getTextMessage();
        final String messageId = messageIdCalculator(textMessage);

        // This will contain the results of all ops in the transaction
        final List<Object> txResults = redisTemplate.execute(new SessionCallback<List<Object>>() {
            public List<Object> execute(final RedisOperations operations) throws DataAccessException {
                operations.multi();
                // Store shard position per channel in Redis Hash:
                textMessage.getHeader().getShardPosition().ifPresent(shardPosition -> {
                    final String channelPosKey = name + "-" + indexedEntry.getChannelName() + "-channelPos";
                    final BoundHashOperations channelPosHash = operations.boundHashOps(channelPosKey);
                    channelPosHash.put(shardPosition.shardName(), shardPosition.position());
                });
                // Store channelName in Redis Set
                final String channelNamesKey = name + "-channels";
                final BoundSetOperations channelNamesSet = operations.boundSetOps(channelNamesKey);
                channelNamesSet.add(indexedEntry.getChannelName());

                // Store every Message as a single Redis Hash '<channelName>-message-<messageId>'
                final String messageHashKey = name + "-message-" + messageId;
                final BoundHashOperations messageHash = operations.boundHashOps(messageHashKey);
                messageHash.putAll(encode(indexedEntry));
                // ...and set the expiration timeout for the message
                messageHash.expire(maxAge, TimeUnit.SECONDS);

                // Add id to the List of all messages of the channel in '<channelName>-messages'
                final String messagesListKey = name + "-messages";
                final BoundListOperations messageList = operations.boundListOps(messagesListKey);
                messageList.rightPush(messageHashKey);
                // ...and set/update the expiration timeout for this list
                messageList.expire(maxAge, TimeUnit.SECONDS);
                // ...and limit the number of entries so it will not grow without bounds
                messageList.trim(-maxSize, -1);

                // Calculate the indexes and add message keys to the different indexes
                indexedEntry.getFilterValues().entrySet().forEach(filterEntry -> {
                    // Add id to the List of all messages of the channel in '<channelName>-messages'
                    final String indexListKey = name + "-" + filterEntry.getKey().getName() + "-" + filterEntry.getValue();
                    final BoundListOperations partitionIndexList = operations.boundListOps(indexListKey);
                    partitionIndexList.rightPush(messageHashKey);
                    // ...and set/update the expiration timeout for this list
                    partitionIndexList.expire(maxAge, TimeUnit.SECONDS);
                });

                return operations.exec();
            }
        });
        LOG.debug("Redis returned with " + txResults);
    }

    @Override
    public Set<String> getChannelNames() {
        Set<String> members = redisTemplate
                .boundSetOps(name + "-channels")
                .members();
        return members;
    }

    @Override
    public ImmutableSet<Index> getIndexes() {
        return indexer.getIndexes();
    }

    @Override
    public ChannelPosition getLatestChannelPosition(final String channelName) {
        final Set<ShardPosition> shardPositions = redisTemplate
                .boundHashOps(name + "-" + channelName + "-channelPos")
                .entries()
                .entrySet()
                .stream()
                .map(entry -> fromPosition(entry.getKey().toString(), entry.getValue().toString()))
                .collect(Collectors.toSet());
        return channelPosition(shardPositions);
    }

    @Override
    public Stream<MessageStoreEntry> stream() {
        final Iterator<MessageStoreEntry> messageIterator = new BatchedRedisHashedListIterator<>(
                redisTemplate,
                this::decode,
                name + "-messages",
                batchSize);
        return StreamSupport.stream(
                spliteratorUnknownSize(messageIterator, CHARACTERISTICS),
                false
        );
    }

    public Stream<MessageStoreEntry> stream(final Index index, final String value) {
        final Iterator<MessageStoreEntry> messageIterator = new BatchedRedisHashedListIterator<>(
                redisTemplate,
                this::decode,
                name + "-" + index.getName() + "-" + value,
                batchSize);
        return StreamSupport.stream(
                spliteratorUnknownSize(messageIterator, CHARACTERISTICS),
                false
        );
    }

    @Override
    public int size() {
        return redisTemplate.boundListOps(name + "-messages").size().intValue();
    }

    @Override
    public void close() {
    }

    public void clear() {
        final List<String> keys = new ArrayList<>(asList(name + "-channels", name + "-messages"));
        getChannelNames().forEach(channel -> keys.add(name + "-" + channel + "-channelPos"));
        redisTemplate.delete(keys);
    }

    private ImmutableMap<String, String> encode(final MessageStoreEntry entry) {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .put("_channelName", entry.getChannelName())
                .put("_message", encoder.apply(entry.getTextMessage()));
        entry.getFilterValues().forEach((key, value) -> builder.put(key.getName(), value));
        return builder.build();
    }

    private MessageStoreEntry decode(final Map<String, String> map) {
        final Map<Index,String> filterValues = map
                .entrySet()
                .stream()
                .filter(this::isFilterValue)
                .collect(toMap(
                        entry -> Index.valueOf(entry.getKey()),
                        entry -> entry.getValue()));
        return MessageStoreEntry.of(
                map.get("_channelName"),
                ImmutableMap.copyOf(filterValues),
                decoder.apply(map.get("_message")));
    }

    private boolean isFilterValue(Map.Entry<String, String> entry) {
        return !entry.getKey().equals("_channelName") && !entry.getKey().equals("_message");
    }

    private final String messageIdCalculator(final TextMessage message) {
        final String msgId = message.getHeader().getAsString(MSG_ID);
        return msgId != null ? msgId : UUID.randomUUID().toString();
    }

}
