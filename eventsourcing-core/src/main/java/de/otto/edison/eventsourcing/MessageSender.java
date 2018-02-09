package de.otto.edison.eventsourcing;


import de.otto.edison.eventsourcing.message.Message;

import java.util.stream.Stream;

@FunctionalInterface
public interface MessageSender {

    <T> void send(String key, T payload);

    default <T> void sendEvents(Stream<Message<T>> messageStream) {
        messageStream.forEach(body -> send(body.getKey(), body.getPayload()));
    }

}
