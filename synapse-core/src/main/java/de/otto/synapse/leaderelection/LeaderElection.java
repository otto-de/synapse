package de.otto.synapse.leaderelection;

import com.google.common.annotations.Beta;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Leader-Election for Synapse services.
 *
 */
@Beta
public interface LeaderElection {

    /**
     * Synchronously executes the runnable, if the current thread is able to become the leader
     * by optaining the specified lock.
     *
     * @param lockName the name of the distributed lock used for leader election
     * @param runnable the runnable that is executed by the leader
     */
    void runIfLeader(String lockName,
                     Runnable runnable);

    /**
     * Synchronously executes the supplier, if the current thread is able to become the leader
     * by optaining the specified lock.
     *
     * @param lockName the name of the distributed lock used for leader election
     * @param supplier the supplier that is executed by the leader
     * @param <T> the type of the object returned by the supplier
     * @return CompletableFuture of the object returned by the supplier, or CompletableFuture with value null,
     *         if the current thread is not the leader
     */
    <T> T supplyIfLeader(String lockName,
                         Supplier<T> supplier);

    /**
     * Asynchronously executes the runnable, if the current thread is able to become the leader
     * by optaining the specified lock.
     *
     * @param lockName the name of the distributed lock used for leader election
     * @param runnable the runnable that is executed by the leader
     * @return CompletableFuture&lt;Void&gt; that can be used to {@link CompletableFuture#join()} or further process
     *         the result.
     */
    CompletableFuture<Void> runAsyncIfLeader(String lockName,
                                             Runnable runnable);

    /**
     * Asynchronously executes the runnable, if the current thread is able to become the leader
     * by optaining the specified lock.
     *
     * @param lockName the name of the distributed lock used for leader election
     * @param runnable the runnable that is executed by the leader
     * @param executor the Executor used to asynchronously run the Runnable
     * @return CompletableFuture&lt;Void&gt; that can be used to {@link CompletableFuture#join()} or further process
     *         the result.
     */
    CompletableFuture<Void> runAsyncIfLeader(String lockName,
                                             Runnable runnable,
                                             Executor executor);

    /**
     * Asynchronously executes the supplier, if the current thread is able to become the leader
     * by optaining the specified lock.
     *
     * @param lockName the name of the distributed lock used for leader election
     * @param supplier the supplier that is executed by the leader
     * @param <T> the type of the object returned by the supplier
     * @return CompletableFuture of the object returned by the supplier, or CompletableFuture with value null,
     *         if the current thread is not the leader
     */
    <T> CompletableFuture<T> supplyAsyncIfLeader(String lockName,
                                                 Supplier<T> supplier);

    /**
     * Asynchronously executes the supplier, if the current thread is able to become the leader
     * by optaining the specified lock.
     *
     * @param lockName the name of the distributed lock used for leader election
     * @param supplier the supplier that is executed by the leader
     * @param executor the Executor used to asynchronously supply the response
     * @param <T> the type of the object returned by the supplier
     * @return CompletableFuture of the object returned by the supplier, or CompletableFuture with value null,
     *         if the current thread is not the leader
     */
    <T> CompletableFuture<T> supplyAsyncIfLeader(String lockName,
                                                 Supplier<T> supplier,
                                                 Executor executor);
}
