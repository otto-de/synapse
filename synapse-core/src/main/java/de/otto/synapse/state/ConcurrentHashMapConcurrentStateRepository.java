package de.otto.synapse.state;

import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashMapConcurrentStateRepository<V> extends ConcurrentStateRepository<V> {

    public ConcurrentHashMapConcurrentStateRepository() {
        super(new ConcurrentHashMap<>());
    }
}
