package de.otto.synapse.messagestore.redis;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreEntry;
import de.otto.synapse.translator.*;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.DefaultHeaderAttr.MSG_ID;
import static de.otto.synapse.message.DefaultHeaderAttr.MSG_RECEIVER_TS;
import static java.util.Arrays.asList;
import static java.util.Spliterators.spliteratorUnknownSize;

/**
 * Redis-based implementation of a WritableMessageStore.
 *
 * <p>
 *     The store can be configured like a ring-buffer to only store the latest N messages.
 * </p>
 */
public class RedisIndexedMessageStore implements MessageStore {

    private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE;

    private final String channelName;
    private final RedisTemplate<String, String> redisTemplate;
    private final int batchSize;
    private final int maxSize;
    private final Encoder<String> encoder;
    private final Decoder<String> decoder;

    /**
     * @param channelName the name of the channel whose messages are stored in the {@code RedisMessageStore}
     * @param batchSize the size of the batches used to fetch messages from Redis
     * @param ringBufferSize the maximum number of messages stored in the ring-buffer
     * @param stringRedisTemplate the RedisTemplate used to access Redis
     */
    public RedisIndexedMessageStore(final String channelName,
                                    final int batchSize,
                                    final int ringBufferSize,
                                    final RedisTemplate<String, String> stringRedisTemplate) {
        this(channelName, batchSize, ringBufferSize, stringRedisTemplate, new TextEncoder(MessageFormat.V2), new TextDecoder());
    }

    /**
     * @param channelName the name of the channel whose messages are stored in the {@code RedisMessageStore}
     * @param batchSize the size of the batches used to fetch messages from Redis
     * @param ringBufferSize the maximum number of messages stored in the ring-buffer
     * @param stringRedisTemplate the RedisTemplate used to access Redis
     * @param messageEncoder the encoder used to encode messages into the string-representation stored in Redis
     */
    public RedisIndexedMessageStore(final String channelName,
                                    final int batchSize,
                                    final int ringBufferSize,
                                    final RedisTemplate<String, String> stringRedisTemplate,
                                    final Encoder<String> messageEncoder,
                                    final Decoder<String> messageDecoder) {
        this.channelName = channelName;
        this.redisTemplate = stringRedisTemplate;
        this.batchSize = batchSize;
        this.maxSize = ringBufferSize;
        this.encoder = messageEncoder;
        this.decoder = messageDecoder;
    }

    public String getChannelName() {
        return channelName;
    }

//    @Override
    public void add(final TextMessage message) {
        final List<Object> txResults = redisTemplate.execute(new SessionCallback<List<Object>>() {
            public List<Object> execute(final RedisOperations operations) throws DataAccessException {
                final String messageId = messageIdCalculator(message);
                operations.multi();

                // Update the channel positition in Hash '<channelName>-channelPos'
                message.getHeader().getShardPosition().ifPresent(shardPosition -> {
                    operations
                            .boundHashOps(channelName + "-channelPos")
                            .put(shardPosition.shardName(), shardPosition.position());
                });


                // Store every Message as a single Redis Hash '<channelName>-message-<messageId>'
                operations
                        .boundHashOps(channelName + "-message-" + messageId)
                        .putAll(ImmutableMap.of(
                                "channelName", channelName,
                                "msgId", messageId,
                                "partitionKey", message.getKey().partitionKey(),
                                "receiverTs", message.getHeader().getAsInstant(MSG_RECEIVER_TS, Instant.now()).toEpochMilli(),
                                "message", encoder.apply(message)
                        ));
                // ...and set the expiration timeout for the message
                operations
                        .boundHashOps(channelName + "-message-" + messageId)
                        .expire(10, TimeUnit.SECONDS);

                // Add id to the List of all messages of the channel in '<channelName>-messages'
                operations
                        .boundListOps(channelName + "-messages")
                        .rightPush(channelName + "-message-" + messageId);
                // ...and set/update the expiration timeout for this list
                operations
                        .boundHashOps(channelName + "-message")
                        .expire(10, TimeUnit.SECONDS);
                // ...and limit the number of entries so it will not grow without bounds
                operations
                        .boundListOps(channelName + "-messages")
                        .trim(-maxSize, -1);

                return operations.exec();
            }
        });
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public Set<String> getChannelNames() {
        return null;
    }

    @Override
    public ChannelPosition getLatestChannelPosition(String channelName) {
        return null;
    }

    @Override
    public ChannelPosition getLatestChannelPosition() {
        final Set<ShardPosition> shardPositions = redisTemplate
                .boundHashOps(channelName + "-channelPos")
                .entries()
                .entrySet()
                .stream()
                .map(entry -> fromPosition(entry.getKey().toString(), entry.getValue().toString()))
                .collect(Collectors.toSet());
        return channelPosition(shardPositions);
    }

    @Override
    public Stream<MessageStoreEntry> streamAll() {
        return null;
    }

//    @Override
    public Stream<TextMessage> stream() {
        final Iterator<TextMessage> messageIterator = new BatchedRedisListIterator<>(
                redisTemplate,
                decoder,
                channelName + "-messages",
                batchSize);
        return StreamSupport.stream(
                spliteratorUnknownSize(messageIterator, CHARACTERISTICS),
                false
        );
    }

    @Override
    public int size() {
        return redisTemplate.boundListOps(channelName + "-messages").size().intValue();
    }

    @Override
    public void close() {
    }

    public void clear() {
        redisTemplate.delete(asList(channelName + "-channelPos", channelName + "-messages"));
    }

    private final String messageIdCalculator(final TextMessage message) {
        final String msgId = message.getHeader().getAsString(MSG_ID);
        return msgId != null ? msgId : UUID.randomUUID().toString();
    }

    @Override
    public void add(MessageStoreEntry entry) {

    }
}
