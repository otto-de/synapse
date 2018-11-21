package de.otto.synapse.endpoint.sender;

import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.translator.JsonStringMessageTranslator;

import javax.annotation.Nonnull;

public class InMemoryMessageSenderFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final InMemoryChannels inMemoryChannels;

    public InMemoryMessageSenderFactory(final MessageInterceptorRegistry registry,
                                        final InMemoryChannels inMemoryChannels) {
        this.registry = registry;
        this.inMemoryChannels = inMemoryChannels;
    }

    @Override
    public InMemoryMessageSender create(@Nonnull final String channelName) {
        final InMemoryMessageSender messageSender = new InMemoryMessageSender(
                registry,
                new JsonStringMessageTranslator(),
                inMemoryChannels.getChannel(channelName));
        return messageSender;
    }

    @Override
    public boolean matches(Class<? extends Selector> channelSelector) {
        // As the in-mem implementation of the factory is intended to replase special implementations
        // and because in-mem channels support both message logs and queues, we can simply return true here:
        return true;
    }

}
