package de.otto.edison.eventsourcing;


import de.otto.edison.eventsourcing.event.EventBody;
import java.util.stream.Stream;

@FunctionalInterface
public interface EventSender {

    <T> void sendEvent(String key, T payload);

    default <T> void sendEvents(Stream<EventBody<T>> events) {
        events.forEach(body -> sendEvent(body.getKey(), body.getPayload()));
    }

}
