package de.otto.synapse.state;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static org.slf4j.LoggerFactory.getLogger;

public class ConcurrentMapStateRepository<V> implements StateRepository<V> {

    private static final Logger LOG = getLogger(ConcurrentMapStateRepository.class);

    private final String name;
    private final ConcurrentMap<String, V> concurrentMap;

    public ConcurrentMapStateRepository(final String name,
                                        final ConcurrentMap<String, V> map) {
        this.name = requireNonNull(name, "Parameter 'name' must not be null");
        this.concurrentMap = requireNonNull(map, "Parameter 'map' must not be null");
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Optional<V> compute(final String key, final BiFunction<? super String, ? super Optional<V>, ? extends V> remappingFunction) {
        return ofNullable(concurrentMap.compute(key, (k, v) -> remappingFunction.apply(k, ofNullable(v))));
    }

    @Override
    public void consumeAll(BiConsumer<? super String, ? super V> consumer) {
        concurrentMap.forEach(consumer);
    }

    @Override
    public Optional<V> put(final String key, final V value) {
        return ofNullable(concurrentMap.put(key, value));
    }

    @Override
    public Optional<V> remove(final String key) {
        return ofNullable(concurrentMap.remove(key));
    }

    @Override
    public void clear() {
        concurrentMap.clear();
    }

    @Override
    public Optional<V> get(final String key) {
        return ofNullable(concurrentMap.get(key));
    }

    @Override
    public ImmutableSet<String> keySet() {
        return ImmutableSet.copyOf(concurrentMap.keySet());
    }

    @Override
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
