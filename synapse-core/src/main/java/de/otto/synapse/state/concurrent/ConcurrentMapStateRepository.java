package de.otto.synapse.state.concurrent;

import de.otto.synapse.state.StateRepository;

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

public class ConcurrentMapStateRepository<V> implements StateRepository<V> {
    protected ConcurrentMap<String, V> concurrentMap;

    public ConcurrentMapStateRepository(ConcurrentMap<String, V> concurrentMap) {
        this.concurrentMap = concurrentMap;
    }

    @Override
    public V compute(String key, BiFunction<? super String, ? super Optional<V>, ? extends V> remappingFunction) {
        return concurrentMap.compute(key, (k, v) -> remappingFunction.apply(k, Optional.ofNullable(v)));
    }

    @Override
    public void put(String key, V value) {
        concurrentMap.put(key, value);
    }

    @Override
    public void remove(String key) {
        concurrentMap.remove(key);
    }

    @Override
    public void clear() {
        concurrentMap.clear();
    }

    @Override
    public Optional<V> get(String key) {
        return Optional.ofNullable(concurrentMap.get(key));
    }

    @Override
    public Iterable<String> getKeySetIterable() {
        return concurrentMap.keySet();
    }

    @Override
    public long size() {
        return concurrentMap.size();
    }

    @Override
    public String getStats() {
        return "nothing";
    }
}
