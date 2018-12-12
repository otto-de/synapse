package de.otto.synapse.endpoint.sender;

import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.translator.JsonStringMessageTranslator;

import javax.annotation.Nonnull;

public class InMemoryMessageSenderFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final InMemoryChannels inMemoryChannels;
    private final Class<? extends Selector> matching;

    public InMemoryMessageSenderFactory(final MessageInterceptorRegistry registry,
                                        final InMemoryChannels inMemoryChannels) {
        this.registry = registry;
        this.inMemoryChannels = inMemoryChannels;
        this.matching = null;
    }

    public InMemoryMessageSenderFactory(final MessageInterceptorRegistry registry,
                                        final InMemoryChannels inMemoryChannels,
                                        final Class<? extends Selector> matching) {
        this.registry = registry;
        this.inMemoryChannels = inMemoryChannels;
        this.matching = matching;
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
        return matching == null || matching.isAssignableFrom(channelSelector);
    }

}
