package de.otto.synapse.sender;


import de.otto.synapse.message.Message;

import java.util.stream.Stream;

@FunctionalInterface
public interface MessageSender {

    <T> void send(Message<T> message);

    default <T> void sendBatch(final Stream<Message<T>> messageStream) {
        messageStream.forEach(this::send);
    }

}
