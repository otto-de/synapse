package de.otto.edison.eventsourcing.inmemory;

import de.otto.edison.eventsourcing.MessageSender;
import de.otto.edison.eventsourcing.message.Message;
import de.otto.edison.eventsourcing.translator.MessageTranslator;

public class InMemoryMessageSender implements MessageSender {

    private final MessageTranslator<String> messageTranslator;
    private final InMemoryChannel channel;

    public InMemoryMessageSender(final MessageTranslator<String> messageTranslator,
                                 final InMemoryChannel channel) {
        this.messageTranslator = messageTranslator;
        this.channel = channel;
    }

    @Override
    public <T> void send(final Message<T> message) {
        channel.send(
                messageTranslator.translate(message)
        );
    }
}
