package de.otto.synapse.state;

import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashMapStateRepository<V> extends ConcurrentMapStateRepository<V> {

    /**
     * Creates a StateRepository with the given name, that is using a {@code ConcurrentHashMap} to store
     * event-sourced entities.
     *
     * @param name the {@link #getName() name}  of the repository.
     */
    public ConcurrentHashMapStateRepository(final String name) {
        super(name, new ConcurrentHashMap<>());
    }
}
