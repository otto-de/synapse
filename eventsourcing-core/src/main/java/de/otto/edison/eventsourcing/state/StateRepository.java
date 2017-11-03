package de.otto.edison.eventsourcing.state;

import java.util.concurrent.ConcurrentMap;

public interface StateRepository<V> extends ConcurrentMap<String,V> {

    // TODO: Statistics / Metrics - kein String
    String getStats();
}
