package de.otto.edison.eventsourcing.state;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Threadsafe default implementation of {@link StateRepository StateRepository&lt;V&gt;} based on a {@link ConcurrentHashMap}.
 *
 * @param <V> the value-type of entries contained int the StateRepository
 */
@ThreadSafe
public class DefaultStateRepository<V> implements StateRepository<V> {

    private final Map<String, V> map = new ConcurrentHashMap<>();

    @Override
    public void put(String key, V value) {
        map.put(key, value);
    }

    @Override
    public Optional<V> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public void remove(String key) {
        map.remove(key);
    }

    @Override
    public Iterable<String> getKeySetIterable() {
        return map.keySet();
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public String getStats() {
        return "";
    }
}
