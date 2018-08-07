package de.otto.synapse.messagequeue;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;

import javax.annotation.Nonnull;

public class InMemoryMessageQueueSender extends AbstractMessageSenderEndpoint {

    private final InMemoryChannel channel;

    public InMemoryMessageQueueSender(final MessageTranslator<String> messageTranslator,
                                      final InMemoryChannel inMemoryChannel) {
        super(inMemoryChannel.getChannelName(), messageTranslator);
        this.channel = inMemoryChannel;
    }

    @Override
    protected void doSend(@Nonnull Message<String> message) {
        channel.send(message);
    }

}
