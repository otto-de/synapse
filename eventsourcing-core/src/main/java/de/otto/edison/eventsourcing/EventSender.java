package de.otto.edison.eventsourcing;


@FunctionalInterface
public interface EventSender {

    void sendEvent(String key, Object payload);


}
