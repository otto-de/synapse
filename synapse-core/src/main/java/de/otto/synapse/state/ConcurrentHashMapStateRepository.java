package de.otto.synapse.state;

import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashMapStateRepository<V> extends ConcurrentMapStateRepository<V> {

    public ConcurrentHashMapStateRepository() {
        super(new ConcurrentHashMap<>());
    }
}
