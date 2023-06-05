package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.InMemoryChannels;
import jakarta.annotation.Nonnull;

/**
 * A builder used to build in-memory implementations of an {@link MessageQueueReceiverEndpoint}.
 * <p>
 *     Primarily used for testing purposes.
 * </p>
 */
public class InMemoryMessageQueueReceiverEndpointFactory implements MessageQueueReceiverEndpointFactory {

    private final InMemoryChannels inMemoryChannels;

    public InMemoryMessageQueueReceiverEndpointFactory(final InMemoryChannels inMemoryChannels) {
        this.inMemoryChannels = inMemoryChannels;
    }

    @Override
    public MessageQueueReceiverEndpoint create(final @Nonnull String channelName) {
        return inMemoryChannels.getChannel(channelName);
    }
}
