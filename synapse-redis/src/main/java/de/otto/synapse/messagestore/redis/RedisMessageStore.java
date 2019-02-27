package de.otto.synapse.messagestore.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.messagestore.MessageStoreEntry;
import de.otto.synapse.messagestore.WritableMessageStore;
import de.otto.synapse.translator.*;
import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

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
public class RedisMessageStore implements WritableMessageStore {

    private static final Logger LOG = getLogger(RedisMessageStore.class);

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
    public RedisMessageStore(final String name,
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
    public RedisMessageStore(final String name,
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
    public void add(final MessageStoreEntry entry) {
        encode(entry);

        // This will contain the results of all ops in the transaction
        final List<Object> txResults = redisTemplate.execute(new SessionCallback<List<Object>>() {
            public List<Object> execute(final RedisOperations operations) throws DataAccessException {
                operations.multi();
                // Put the Message as a Redis Hash:
                entry.getTextMessage().getHeader().getShardPosition().ifPresent(shardPosition -> {
                    operations
                            .boundHashOps(name + "-" + entry.getChannelName() + "-channelPos")
                            .put(shardPosition.shardName(), shardPosition.position());
                });
                operations
                        .boundSetOps(name + "-channels")
                        .add(entry.getChannelName());
                operations
                        .boundListOps(name + "-messages")
                        .rightPush(encode(entry));
                operations
                        .boundListOps(name + "-messages")
                        .trim(-maxSize, -1);
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
        redisTemplate.delete(asList(name + "-channelPos", name + "-messages"));
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
