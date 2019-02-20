package de.otto.synapse.leaderelection.redis;

import com.google.common.annotations.Beta;
import de.otto.synapse.leaderelection.LeaderElection;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.slf4j.LoggerFactory.getLogger;

@Beta
public class RedisLeaderElection implements LeaderElection {

    private static final Logger LOG = getLogger(RedisLeaderElection.class);

    private final RedissonClient redissonClient;

    public RedisLeaderElection(final RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Override
    public final CompletableFuture<Void> runAsyncIfLeader(final String lockName,
                                                          final Runnable runnable) {
        return runAsync(() -> runIfLeader(lockName, runnable));
    }

    @Override
    public final CompletableFuture<Void> runAsyncIfLeader(final String lockName,
                                                          final Runnable runnable,
                                                          final Executor executor) {
        return runAsync(() -> runIfLeader(lockName, runnable), executor);
    }

    @Override
    public final <T> CompletableFuture<T> supplyAsyncIfLeader(final String lockName,
                                                              final Supplier<T> supplier) {
        return CompletableFuture.supplyAsync(() -> supplyIfLeader(lockName, supplier));
    }

    @Override
    public final <T> CompletableFuture<T> supplyAsyncIfLeader(final String lockName,
                                                              final Supplier<T> supplier,
                                                              final Executor executor) {
        return CompletableFuture.supplyAsync(() -> supplyIfLeader(lockName, supplier), executor);
    }

    @Override
    public final void runIfLeader(final String lockName,
                                  final Runnable runnable) {
        supplyIfLeader(lockName, () -> {
            runnable.run();
            return null;
        });
    }

    @Override
    public final <T> T supplyIfLeader(final String lockName,
                                      final Supplier<T> supplier) {
        final RLock lock = redissonClient.getLock(lockName);
        T result = null;
        try {
            LOG.info("Waiting for becoming leader...");
            if (lock.tryLock(5, TimeUnit.SECONDS)) {
                LOG.info("Thread {} is leader for {} ", currentThread().getName(), lock.getName());
                try {
                    result = supplier.get();
                } finally {
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                        LOG.info("Released leader-election lock {}", currentThread().getName(), lock.getName());
                    }
                }
            }
        } catch (final InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
        return result;
    }

}
