package de.otto.synapse.messagestore.redis;

import com.google.common.annotations.Beta;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 *
 * @param <R> The type of the value stored in Redis
 */
@Beta
class BatchedRedisHashedListIterator<R> implements Iterator<R> {
    private final RedisTemplate<String,String> redisTemplate;
    private final String redisListName;
    private final Function<Map<String,String>, R> valueTransformer;


    private final BatchedRedisListIterator<String, String> messageListIterator;
    private R next;

    BatchedRedisHashedListIterator(final RedisTemplate<String, String> redisTemplate,
                                   final Function<Map<String, String>, R> valueTransformer,
                                   final String redisListName,
                                   final int batchSize) {
        this.valueTransformer = valueTransformer;
        requireNonNull(redisTemplate, "Parameter redisTemplate must not be null");
        if (batchSize < 1) {
            throw new IllegalArgumentException("Parameter batchSize must be greater 0");
        }
        if (isNullOrEmpty(redisListName)) {
            throw new IllegalArgumentException("Parameter redisListName must not be empty");
        }
        this.redisTemplate = redisTemplate;
        this.redisListName = redisListName;
        messageListIterator = new BatchedRedisListIterator<>(redisTemplate, Function.identity(), redisListName, batchSize);
        next = null;
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            if (messageListIterator.hasNext()) {
                final String messageHashKey = messageListIterator.next();
                final BoundHashOperations<String, String, String> hashOps = redisTemplate.boundHashOps(messageHashKey);
                final Map<String, String> entries = hashOps.entries();
                if (entries != null && !entries.isEmpty()) {
                    next = valueTransformer.apply(entries);
                }
            }
        }
        return next != null;
    }

    @Override
    public R next() {
        if (hasNext()) {
            R element = this.next;
            this.next = null;
            return element;
        } else {
            throw new NoSuchElementException("No more messages available in " + redisListName);
        }
    }

}
