package de.otto.synapse.messagestore.redis;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.WritableMessageStore;
import de.otto.synapse.translator.*;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static java.util.Arrays.asList;
import static java.util.Spliterators.spliteratorUnknownSize;

/**
 * Redis-based implementation of a WritableMessageStore.
 *
 * <p>
 *     The store can be configured like a ring-buffer to only store the latest N messages.
 * </p>
 */
public class RedisMessageStore implements WritableMessageStore {

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
    public RedisMessageStore(final String channelName,
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
    public RedisMessageStore(final String channelName,
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

    @Override
    public void add(final TextMessage message) {
        final List<Object> txResults = redisTemplate.execute(new SessionCallback<List<Object>>() {
            public List<Object> execute(final RedisOperations operations) throws DataAccessException {
                operations.multi();
                // Put the Message as a Redis Hash:
                message.getHeader().getShardPosition().ifPresent(shardPosition -> {
                    operations
                            .boundHashOps(channelName + "-channelPos")
                            .put(shardPosition.shardName(), shardPosition.position());
                });
                operations
                        .boundListOps(channelName + "-messages")
                        .rightPush(encoder.apply(message));
                operations
                        .boundListOps(channelName + "-messages")
                        .trim(-maxSize, -1);
                //operations.boundHashOps(messageId).putAll(hashOf(message));
                //operations.boundZSetOps(channelName + "-timeseries").add(messageId, scoreOf(message));
                // This will contain the results of all ops in the transaction
                return operations.exec();
            }
        });
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
}
