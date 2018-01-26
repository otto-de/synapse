package de.otto.edison.eventsourcing;

import de.otto.edison.eventsourcing.inmemory.Tuple;

@FunctionalInterface
public interface EventSender {

    void sendEvent(String key, Object payload);


}
