package de.otto.synapse.messagestore.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreEntry;
import de.otto.synapse.translator.*;
import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;
import static java.util.Arrays.asList;
import static java.util.Spliterators.spliteratorUnknownSize;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Redis-based implementation of a WritableMessageStore.
 *
 * <p>
 *     The store can be configured like a ring-buffer to only store the latest N messages.
 * </p>
 */
@Beta
public class RedisRingBufferMessageStore implements MessageStore {

    private static final Logger LOG = getLogger(RedisRingBufferMessageStore.class);

    private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE;

    private final String name;
    private final RedisTemplate<String, String> redisTemplate;
    private final int batchSize;
    private final int maxSize;
    private final Encoder<String> encoder;
    private final Decoder<String> decoder;

    /**
     * @param name the name of the message store
     * @param batchSize the size of the batches used to fetch messages from Redis
     * @param ringBufferSize the maximum number of messages stored in the ring-buffer
     * @param stringRedisTemplate the RedisTemplate used to access Redis
     */
    public RedisRingBufferMessageStore(final String name,
                                       final int batchSize,
                                       final int ringBufferSize,
                                       final RedisTemplate<String, String> stringRedisTemplate) {
        this(name, batchSize, ringBufferSize, stringRedisTemplate, new TextEncoder(MessageFormat.V2), new TextDecoder());
    }

    /**
     * @param name the name of the message store
     * @param batchSize the size of the batches used to fetch messages from Redis
     * @param ringBufferSize the maximum number of messages stored in the ring-buffer
     * @param stringRedisTemplate the RedisTemplate used to access Redis
     * @param messageEncoder the encoder used to encode messages into the string-representation stored in Redis
     */
    public RedisRingBufferMessageStore(final String name,
                                       final int batchSize,
                                       final int ringBufferSize,
                                       final RedisTemplate<String, String> stringRedisTemplate,
                                       final Encoder<String> messageEncoder,
                                       final Decoder<String> messageDecoder) {
        this.name = name;
        this.redisTemplate = stringRedisTemplate;
        this.batchSize = batchSize;
        this.maxSize = ringBufferSize;
        this.encoder = messageEncoder;
        this.decoder = messageDecoder;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(final MessageStoreEntry entry) {
        // This will contain the results of all ops in the transaction
        final List<Object> txResults = redisTemplate.execute(new SessionCallback<List<Object>>() {
            public List<Object> execute(final RedisOperations operations) throws DataAccessException {
                operations.multi();

                // Store shard position per channel in Redis Hash:
                entry.getTextMessage().getHeader().getShardPosition().ifPresent(shardPosition -> {
                    final String channelPosHashKey = name + "-" + entry.getChannelName() + "-channelPos";
                    final BoundHashOperations channelPosHash = operations.boundHashOps(channelPosHashKey);
                    channelPosHash.put(shardPosition.shardName(), shardPosition.position());
                });

                // Store channelName in Redis Set
                final String channelNamesSetKey = name + "-channels";
                final BoundSetOperations channelNamesSet = operations.boundSetOps(channelNamesSetKey);
                channelNamesSet.add(entry.getChannelName());

                // Encode entry into a string and store it in a Redis list:
                final String messagesListKey = name + "-messages";
                final BoundListOperations messagesList = operations.boundListOps(messagesListKey);
                messagesList.rightPush(encode(entry));
                // Trim the list to <maxSize> elements
                messagesList.trim(-maxSize, -1);

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
    public Stream<MessageStoreEntry> streamAll() {
        final Iterator<MessageStoreEntry> messageIterator = new BatchedRedisListIterator<>(
                redisTemplate,
                this::decode,
                name + "-messages",
                batchSize
        );
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

    private String encode(final MessageStoreEntry entry) {
        try {
            return currentObjectMapper().writeValueAsString(ImmutableMap.of(
                    "channelName", entry.getChannelName(),
                    "message", encoder.apply(entry.getTextMessage())));
        } catch (final JsonProcessingException e) {
            throw new IllegalStateException("Failed to encode MessageStoreEntry " + entry + ": " + e.getMessage(), e);
        }
    }

    private MessageStoreEntry decode(final String value) {
        try {
            final Map map = currentObjectMapper().readValue(value, Map.class);
            return MessageStoreEntry.of(
                    map.get("channelName").toString(),
                    decoder.apply(map.get("message").toString()));
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to decode MessageStoreEntry from " + value + ": " + e.getMessage(), e);
        }
    }
}
