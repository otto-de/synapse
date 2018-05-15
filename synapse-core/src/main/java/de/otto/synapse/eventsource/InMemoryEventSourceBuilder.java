package de.otto.synapse.eventsource;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.springframework.context.ApplicationEventPublisher;

/**
 * A builder used to build in-memory implementations of an {@link EventSource}.
 * <p>
 *     Primarily used for testing purposes.
 * </p>
 */
public class InMemoryEventSourceBuilder implements EventSourceBuilder {

    private final MessageInterceptorRegistry interceptorRegistry;
    private final InMemoryChannels inMemoryChannels;
    private final ApplicationEventPublisher eventPublisher;

    public InMemoryEventSourceBuilder(final MessageInterceptorRegistry interceptorRegistry,
                                      final InMemoryChannels inMemoryChannels,
                                      final ApplicationEventPublisher eventPublisher) {

        this.interceptorRegistry = interceptorRegistry;
        this.inMemoryChannels = inMemoryChannels;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public EventSource buildEventSource(final String name,
                                        final String channelName) {
        final InMemoryChannel channel = inMemoryChannels.getChannel(channelName);
        channel.registerInterceptorsFrom(interceptorRegistry);
        return new InMemoryEventSource(name, channel, eventPublisher);

    }
}
