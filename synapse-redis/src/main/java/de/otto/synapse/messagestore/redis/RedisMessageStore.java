package de.otto.synapse.messagestore.redis;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;
import de.otto.synapse.messagestore.WritableMessageStore;
import de.otto.synapse.translator.MessageCodec;
import de.otto.synapse.translator.MessageFormat;
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

public class RedisMessageStore implements WritableMessageStore {

    private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE;

    private final String channelName;
    private final RedisTemplate<String, String> redisTemplate;
    private final int batchSize;
    private final int maxSize;

    public RedisMessageStore(final String channelName,
                             final RedisTemplate<String, String> stringRedisTemplate,
                             final int batchSize,
                             final int maxSize) {
        this.channelName = channelName;
        this.redisTemplate = stringRedisTemplate;
        this.batchSize = batchSize;
        this.maxSize = maxSize;
    }

    public String getChannelName() {
        return channelName;
    }

    @Override
    public void add(final Message<String> message) {
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
                        .rightPush(toRedisValue(message));
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
    public Stream<Message<String>> stream() {
        final Iterator<Message<String>> messageIterator = new BatchedRedisListIterator<>(
                redisTemplate,
                RedisMessageStore::messageOf,
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

    static Message<String> messageOf(final String redisValue) {
        return MessageCodec.decode(redisValue, Header.builder(), Message.builder(String.class));
    }

    static String toRedisValue(final Message<String> message) {
        return MessageCodec.encode(message, MessageFormat.V2);
    }

    public void clear() {
        redisTemplate.delete(asList(channelName + "-channelPos", channelName + "-messages"));
    }
}
