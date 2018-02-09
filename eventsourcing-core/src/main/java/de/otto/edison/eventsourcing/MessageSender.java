package de.otto.edison.eventsourcing;


import de.otto.edison.eventsourcing.message.Message;

import java.util.stream.Stream;

import static de.otto.edison.eventsourcing.message.Message.message;

@FunctionalInterface
public interface MessageSender {

    <T> void send(Message<T> message);

    default <T> void send(String key, T payload) {
        send(message(key, payload));
    }

    default <T> void sendBatch(Stream<Message<T>> messageStream) {
        messageStream.forEach(this::send);
    }

}
