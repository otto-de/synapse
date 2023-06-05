package de.otto.synapse.endpoint.sender;

import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.TextMessageTranslator;
import jakarta.annotation.Nonnull;

public class InMemoryMessageSenderFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final InMemoryChannels inMemoryChannels;
    private final Class<? extends Selector> selector;

    public InMemoryMessageSenderFactory(final MessageInterceptorRegistry registry,
                                        final InMemoryChannels inMemoryChannels,
                                        final Class<? extends Selector> selector) {
        this.registry = registry;
        this.inMemoryChannels = inMemoryChannels;
        this.selector = selector;
    }

    @Override
    public InMemoryMessageSender create(@Nonnull final String channelName, MessageFormat messageFormat) {
        final InMemoryMessageSender messageSender = new InMemoryMessageSender(
                registry,
                new TextMessageTranslator(),
                inMemoryChannels.getChannel(channelName));
        return messageSender;
    }

    @Override
    public boolean matches(Class<? extends Selector> channelSelector) {
        return selector == null || selector.isAssignableFrom(channelSelector);
    }

    @Override
    public Class<? extends Selector> selector() {
        return selector;
    }
}
