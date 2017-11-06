package de.otto.edison.eventsourcing.state;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

public interface StateRepository<V> {

    void put(String key, V value);

    void remove(String key);

    Optional<V> get(String key);

    Iterable<String> getKeySetIterable();

    default V compute(String key,
                      BiFunction<? super String, ? super Optional<V>, ? extends V> remappingFunction) {
        Objects.requireNonNull(remappingFunction);
        Optional<V> oldValue = get(key);

        V newValue = remappingFunction.apply(key, oldValue);
        if (newValue == null) {
            // delete mapping
            if (oldValue.isPresent()) {
                // something to remove
                remove(key);
                return null;
            } else {
                // nothing to do. Leave things as they were.
                return null;
            }
        } else {
            // add or replace old mapping
            put(key, newValue);
            return newValue;
        }
    }

    long size();

    // TODO: Statistics / Metrics - kein String
    String getStats();
}
