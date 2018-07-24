package de.otto.synapse.messagequeue;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;

import javax.annotation.Nonnull;

public class InMemoryMessageQueueSender extends AbstractMessageSenderEndpoint {

    private final InMemoryQueueChannel channel;

    public InMemoryMessageQueueSender(final MessageTranslator<String> messageTranslator,
                                      final InMemoryQueueChannel inMemoryQueueChannel) {
        super(inMemoryQueueChannel.getChannelName(), messageTranslator);
        this.channel = inMemoryQueueChannel;
    }

    @Override
    protected void doSend(@Nonnull Message<String> message) {
        channel.send(message);
    }

}
