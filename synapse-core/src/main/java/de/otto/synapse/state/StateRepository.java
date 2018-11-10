package de.otto.synapse.state;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

import static java.util.Optional.ofNullable;
import static org.slf4j.LoggerFactory.getLogger;

public class StateRepository<V> implements AutoCloseable {

    private static final Logger LOG = getLogger(StateRepository.class);

    private ConcurrentMap<String, V> concurrentMap;

    public StateRepository(final ConcurrentMap<String, V> concurrentMap) {
        this.concurrentMap = concurrentMap;
    }

    public V compute(final String key,
                     final BiFunction<? super String, ? super Optional<V>, ? extends V> remappingFunction) {
        return concurrentMap.compute(key, (k, v) -> remappingFunction.apply(k, ofNullable(v)));
    }

    public V put(final String key,
                 final V value) {
        return concurrentMap.put(key, value);
    }

    public void remove(final String key) {
        concurrentMap.remove(key);
    }

    public void clear() {
        concurrentMap.clear();
    }

    public Optional<V> get(final String key) {
        return ofNullable(concurrentMap.get(key));
    }

    public Set<String> keySet() {
        return concurrentMap.keySet();
    }

    public long size() {
        return concurrentMap.size();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing StateRepository.");
        if (concurrentMap instanceof AutoCloseable) {
            ((AutoCloseable) concurrentMap).close();
        }
    }
}
