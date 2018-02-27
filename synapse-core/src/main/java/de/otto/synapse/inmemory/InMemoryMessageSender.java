package de.otto.synapse.inmemory;

import de.otto.synapse.MessageSender;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;

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
