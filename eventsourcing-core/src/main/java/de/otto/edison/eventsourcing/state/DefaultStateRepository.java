package de.otto.edison.eventsourcing.state;

import java.util.concurrent.ConcurrentHashMap;

public class DefaultStateRepository<V> extends ConcurrentHashMap<String, V> implements StateRepository<V> {
    @Override
    public String getStats() {
        return "";
    }
}
