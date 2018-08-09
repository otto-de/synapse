package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpointFactory;

import javax.annotation.Nonnull;

/**
 * A builder used to build in-memory implementations of an {@link MessageQueueReceiverEndpoint}.
 * <p>
 *     Primarily used for testing purposes.
 * </p>
 */
public class InMemoryMessageQueueReceiverEndpointFactory implements MessageQueueReceiverEndpointFactory {

    private final MessageInterceptorRegistry interceptorRegistry;
    private final InMemoryChannels inMemoryChannels;

    public InMemoryMessageQueueReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                       final InMemoryChannels inMemoryChannels) {

        this.interceptorRegistry = interceptorRegistry;
        this.inMemoryChannels = inMemoryChannels;
    }

    @Override
    public MessageQueueReceiverEndpoint create(final @Nonnull String channelName) {
        final InMemoryChannel channel = inMemoryChannels.getChannel(channelName);
        channel.registerInterceptorsFrom(interceptorRegistry);
        return channel;
    }
}
