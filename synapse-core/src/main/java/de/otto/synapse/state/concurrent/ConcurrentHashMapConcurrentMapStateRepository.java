package de.otto.synapse.state.concurrent;

import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashMapConcurrentMapStateRepository<V> extends ConcurrentMapStateRepository<V> {

    public ConcurrentHashMapConcurrentMapStateRepository() {
        super(new ConcurrentHashMap<>());
    }
}
