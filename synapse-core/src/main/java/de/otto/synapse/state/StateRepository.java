package de.otto.synapse.state;

import com.google.common.collect.ImmutableSet;
import de.otto.synapse.messagestore.Index;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * A {@code StateRepository} is used to store the aggregated state of event-sourced entities.
 *
 * @param <V> The type of the event-sourced entities stored in the {@code StateRepository}
 */
public interface StateRepository<V> extends AutoCloseable {

    /**
     * The unique name of the {@code StateRepository}.
     *
     * <p>Using the same name for multiple repositories will lead to </p>
     * @return name
     */
    String getName();

    /**
     * Returns the set of {@link Index indexes} for entities stored in the {@code StateRepository},
     * or an empty set, if no indexers will be created.
     *
     * @return set of indexes
     */
    default ImmutableSet<Index> getIndexes() {
        return ImmutableSet.of();
    }

    /**
     * Returns an immutable set of the keys in this repository.
     *
     * @return a set view of the keys contained in this repository
     */
    Set<String> keySet();

    /**
     * Returns the optional value to which the specified key is mapped,
     * or {@code Optional.empty()} if this repository contains no mapping for the key.
     *
     * <p>More formally, if this repository contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code (key==null ? k==null :
     * key.equals(k))}, then this method returns {@code Optional.of(v)}; otherwise
     * it returns {@code Optional.empty()}.  (There can be at most one such mapping.)
     *
     * @param key the key whose associated value is to be returned
     * @return the Optional containing the value to which the specified key is mapped, or
     * {@code Optional.empty()} if this repository contains no mapping for the key
     * @throws NullPointerException if the specified key is null
     */
    Optional<V> get(String key);

    /**
     * Computes each entry within the repository.
     *
     * @param consumer the consumer that will be applied on each repository entry
     */
    void consumeAll(BiConsumer<? super String, ? super V> consumer);

    /**
     * Associates the specified value with the specified key in this repository.
     * If the repository previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A repository
     * <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #get(String) m.get(k)} would return
     * a non-empty value.)
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>Optional.empty()</tt> if there was no mapping for <tt>key</tt>.
     * @throws ClassCastException       if the class of the specified value
     *                                  prevents it from being stored in this repository
     * @throws NullPointerException     if the specified key or value is null
     * @throws IllegalArgumentException if some property of the specified key
     *                                  or value prevents it from being stored in this repository
     */
    Optional<V> put(String key, V value);

    /**
     * Attempts to compute a mapping for the specified key and its current
     * mapped value (or {@code null} if there is no current mapping). For
     * example, to either create or append a {@code String} msg to a value
     * mapping:
     *
     * <pre> {@code
     * repository.compute(key, (k, v) -> (v == null) ? msg : v.concat(msg))}</pre>
     *
     * <p>If the function returns {@code null}, the mapping is removed (or
     * remains absent if initially absent).  If the function itself throws an
     * (unchecked) exception, the exception is rethrown, and the current mapping
     * is left unchanged.
     * <p>
     * The default implementation is equivalent to performing the following
     * steps for this {@code repository}, then returning the current value or
     * {@code null} if absent:
     *
     * <pre> {@code
     * OptionalV> oldValue = repository.get(key);
     * V newValue = remappingFunction.apply(key, oldValue);
     * if (!oldValue.isEmpty()) {
     *    if (newValue != null)
     *       repository.put(key, newValue);
     *    else
     *       repository.remove(key);
     * } else {
     *    if (newValue != null)
     *       repository.put(key, newValue);
     *    else
     *       return Optional.empty();
     * }
     * }</pre>
     *
     * @param key               key with which the specified value is to be associated
     * @param remappingFunction the function to compute a value
     * @return the Optional containing the new value associated with the specified key, or Optional.empty() if none
     * @throws NullPointerException if the specified key is null, or the
     *                              remappingFunction is null
     * @throws ClassCastException   if the class of the specified value
     *                              prevents it from being stored in this repository
     */
    Optional<V> compute(String key, BiFunction<? super String, ? super Optional<V>, ? extends V> remappingFunction);

    /**
     * Removes the mapping for a key from this repository if it is present.
     * More formally, if this repository contains a mapping
     * from key <tt>k</tt> to value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping
     * is removed.  (The repository can contain at most one such mapping.)
     *
     * <p>Returns the optional value to which this repository previously associated the key,
     * or <tt>Optional.empty()</tt> if the repository contained no mapping for the key.
     *
     * <p>The repository will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param key key whose mapping is to be removed from the repository
     * @return the optional previous value associated with <tt>key</tt>, or
     * <tt>Optional.empty()</tt> if there was no mapping for <tt>key</tt>.
     * @throws NullPointerException if the specified key is null
     */
    Optional<V> remove(String key);

    /**
     * Removes all of the mappings from this repository (optional operation).
     * The repository will be empty after this call returns.
     *
     * @throws UnsupportedOperationException if the <tt>clear</tt> operation
     *                                       is not supported by this repository
     */
    void clear();

    /**
     * Returns the number of key-value mappings in this repository.  If the
     * repository contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of key-value mappings in this repository
     */
    long size();

    /**
     * Closes the {@code StateRepository}.
     *
     * <p>Depending on the implementation of the interface, it might be required to close() the repository on
     * shutdown in order to prevent data loss.</p>
     *
     * @throws Exception if closing the repository fails for some reason.
     */
    @Override
    void close() throws Exception;
}
