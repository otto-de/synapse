package de.otto.synapse.state;

import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class DelegatingStateRepository<V> implements StateRepository<V> {

    private final StateRepository<V> delegate;

    public DelegatingStateRepository(final StateRepository<V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<V> compute(String key, BiFunction<? super String, ? super Optional<V>, ? extends V> remappingFunction) {
        return delegate.compute(key, remappingFunction);
    }

    @Override
    public void consumeAll(BiConsumer<? super String, ? super V> consumer) {
        delegate.consumeAll(consumer);
    }

    @Override
    public Optional<V> put(String key, V value) {
        return delegate.put(key,value);
    }

    @Override
    public Optional<V> remove(String key) {
        return delegate.remove(key);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Optional<V> get(String key) {
        return Optional.empty();
    }

    @Override
    public ImmutableSet<String> keySet() {
        return delegate.keySet();
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
