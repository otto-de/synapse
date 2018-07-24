package de.otto.synapse.messagequeue;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpointFactory;
import de.otto.synapse.eventsource.EventSource;

import javax.annotation.Nonnull;

/**
 * A builder used to build in-memory implementations of an {@link MessageQueueReceiverEndpoint}.
 * <p>
 *     Primarily used for testing purposes.
 * </p>
 */
public class InMemoryMessageQueueReceiverEndpointFactory implements MessageQueueReceiverEndpointFactory {

    private final MessageInterceptorRegistry interceptorRegistry;
    private final InMemoryQueueChannels inMemoryQueueChannels;

    public InMemoryMessageQueueReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                       final InMemoryQueueChannels inMemoryQueueChannels) {

        this.interceptorRegistry = interceptorRegistry;
        this.inMemoryQueueChannels = inMemoryQueueChannels;
    }

    @Override
    public MessageQueueReceiverEndpoint create(final @Nonnull String channelName) {
        final InMemoryQueueChannel channel = inMemoryQueueChannels.getChannel(channelName);
        channel.registerInterceptorsFrom(interceptorRegistry);
        return channel;
    }
}
