package de.otto.synapse.endpoint.sender;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;

import javax.annotation.Nonnull;

public class InMemoryMessageSender extends AbstractMessageSenderEndpoint {

    private final InMemoryChannel channel;

    public InMemoryMessageSender(final MessageTranslator<String> messageTranslator,
                                 final InMemoryChannel channel) {
        super(channel.getChannelName(), messageTranslator);
        this.channel = channel;
    }

    @Override
    protected void doSend(@Nonnull Message<String> message) {
        channel.send(message);
    }

}
