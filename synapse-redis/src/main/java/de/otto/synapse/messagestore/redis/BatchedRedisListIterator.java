package de.otto.synapse.messagestore.redis;

import org.springframework.data.redis.core.RedisTemplate;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 *
 * @param <R> The type of the value stored in Redis
 * @param <T> The type of the objects returned by the iterator
 */
class BatchedRedisListIterator<R, T> implements Iterator<R> {
    private final RedisTemplate<String,T> redisTemplate;
    private final String redisListName;
    private final Function<T, R> valueTransformer;
    private Iterator<T> currentBatchIterator;
    private final int batchSize;
    private int currentBatchPos;
    private int nextBatch;

    BatchedRedisListIterator(final RedisTemplate<String, T> redisTemplate,
                             final Function<T, R> valueTransformer,
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
        this.currentBatchIterator = redisTemplate.boundListOps(this.redisListName).range(0, batchSize - 1).iterator();
        this.currentBatchPos = 0;
        this.batchSize = batchSize;
        this.nextBatch = 1;
    }

    @Override
    public boolean hasNext() {
        if (!currentBatchIterator.hasNext() && currentBatchPos == batchSize) {
            int start = batchSize * nextBatch;
            int end = batchSize * nextBatch + batchSize - 1;
            currentBatchIterator = redisTemplate
                    .boundListOps(redisListName)
                    .range(start, end)
                    .iterator();
            this.currentBatchPos = 0;
            ++nextBatch;
        }
        return currentBatchIterator.hasNext();
    }

    @Override
    public R next() {
        if (hasNext()) {
            ++currentBatchPos;
            return valueTransformer.apply(currentBatchIterator.next());
        } else {
            throw new NoSuchElementException("No more messages available in " + redisListName);
        }
    }

}
