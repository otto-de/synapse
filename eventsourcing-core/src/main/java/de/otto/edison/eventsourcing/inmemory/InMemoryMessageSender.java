package de.otto.edison.eventsourcing.inmemory;

import de.otto.edison.eventsourcing.MessageSender;
import de.otto.edison.eventsourcing.message.Message;
import de.otto.edison.eventsourcing.translator.MessageTranslator;

public class InMemoryMessageSender implements MessageSender {

    private final MessageTranslator<String> messageTranslator;
    private final InMemoryStream eventStream;

    public InMemoryMessageSender(final MessageTranslator<String> messageTranslator,
                                 final InMemoryStream eventStream) {
        this.messageTranslator = messageTranslator;
        this.eventStream = eventStream;
    }

    @Override
    public <T> void send(final Message<T> message) {
        eventStream.send(
                messageTranslator.translate(message)
        );
    }
}
