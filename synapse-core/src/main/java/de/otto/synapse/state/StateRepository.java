package de.otto.synapse.state;

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

public class StateRepository<V> {
    protected ConcurrentMap<String, V> concurrentMap;

    public StateRepository(ConcurrentMap<String, V> concurrentMap) {
        this.concurrentMap = concurrentMap;
    }

    public V compute(String key, BiFunction<? super String, ? super Optional<V>, ? extends V> remappingFunction) {
        return concurrentMap.compute(key, (k, v) -> remappingFunction.apply(k, Optional.ofNullable(v)));
    }

    public V put(String key, V value) {
        return concurrentMap.put(key, value);
    }

    public void remove(String key) {
        concurrentMap.remove(key);
    }

    public void clear() {
        concurrentMap.clear();
    }

    public Optional<V> get(String key) {
        return Optional.ofNullable(concurrentMap.get(key));
    }

    public Iterable<String> getKeySetIterable() {
        return concurrentMap.keySet();
    }

    public long size() {
        return concurrentMap.size();
    }
}
