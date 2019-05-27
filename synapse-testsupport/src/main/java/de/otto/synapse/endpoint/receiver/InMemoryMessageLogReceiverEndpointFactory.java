package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.channel.StartFrom;
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

    public InMemoryMessageLogReceiverEndpointFactory(final InMemoryChannels inMemoryChannels) {
        this.inMemoryChannels = inMemoryChannels;
    }

    @Override
    public MessageLogReceiverEndpoint create(@Nonnull String channelName) {
        return inMemoryChannels.getChannel(channelName);
    }

    @Override
    public MessageLogReceiverEndpoint create(final @Nonnull String channelName, final @Nonnull StartFrom iteratorAt) {
        return inMemoryChannels.getChannel(channelName);
    }
}
