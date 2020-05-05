package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.eventsource.EventSource;

import javax.annotation.Nonnull;

/**
 * A builder used to build in-memory implementations of an {@link EventSource}.
 * <p>
 *     Primarily used for testing purposes.
 * </p>
 */
public class InMemoryMessageLogReceiverEndpointFactory implements MessageLogReceiverEndpointFactory {

    private final InMemoryChannels inMemoryChannels;
    private final Class<? extends Selector> selector;

    public InMemoryMessageLogReceiverEndpointFactory(final InMemoryChannels inMemoryChannels,
                                                     final Class<? extends Selector> selector) {
        this.inMemoryChannels = inMemoryChannels;
        this.selector = selector;
    }

    @Override
    public MessageLogReceiverEndpoint create(final @Nonnull String channelName) {
        return inMemoryChannels.getChannel(channelName);
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
