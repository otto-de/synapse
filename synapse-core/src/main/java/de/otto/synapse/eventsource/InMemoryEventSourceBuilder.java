package de.otto.synapse.eventsource;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;

/**
 * A builder used to build in-memory implementations of an {@link EventSource}.
 * <p>
 *     Primarily used for testing purposes.
 * </p>
 */
public class InMemoryEventSourceBuilder implements EventSourceBuilder {

    private final MessageInterceptorRegistry interceptorRegistry;
    private final InMemoryChannels inMemoryChannels;

    public InMemoryEventSourceBuilder(final MessageInterceptorRegistry interceptorRegistry,
                                      final InMemoryChannels inMemoryChannels) {

        this.interceptorRegistry = interceptorRegistry;
        this.inMemoryChannels = inMemoryChannels;
    }

    @Override
    public EventSource buildEventSource(final String name,
                                        final String channelName) {
        final InMemoryChannel channel = inMemoryChannels.getChannel(channelName);
        channel.registerInterceptorsFrom(interceptorRegistry);
        return new InMemoryEventSource(name, channel);

    }
}
