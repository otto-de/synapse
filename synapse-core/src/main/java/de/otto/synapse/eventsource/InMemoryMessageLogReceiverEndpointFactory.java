package de.otto.synapse.eventsource;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;

import javax.annotation.Nonnull;

/**
 * A builder used to build in-memory implementations of an {@link EventSource}.
 * <p>
 *     Primarily used for testing purposes.
 * </p>
 */
public class InMemoryMessageLogReceiverEndpointFactory implements MessageLogReceiverEndpointFactory {

    private final MessageInterceptorRegistry interceptorRegistry;
    private final InMemoryChannels inMemoryChannels;

    public InMemoryMessageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                     final InMemoryChannels inMemoryChannels) {

        this.interceptorRegistry = interceptorRegistry;
        this.inMemoryChannels = inMemoryChannels;
    }

    @Override
    public MessageLogReceiverEndpoint create(final @Nonnull String channelName) {
        final InMemoryChannel channel = inMemoryChannels.getChannel(channelName);
        channel.registerInterceptorsFrom(interceptorRegistry);
        return channel;
    }
}
