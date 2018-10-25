package de.otto.synapse.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.springframework.context.ApplicationEventPublisher;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryChannels {

    private final ConcurrentMap<String,InMemoryChannel> channels = new ConcurrentHashMap<>();
    private final MessageInterceptorRegistry interceptorRegistry;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;

    public InMemoryChannels(final MessageInterceptorRegistry interceptorRegistry,
                            final ObjectMapper objectMapper,
                            final ApplicationEventPublisher eventPublisher) {
        this.interceptorRegistry = interceptorRegistry;
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
    }

    public InMemoryChannel getChannel(final String channelName) {
        channels.putIfAbsent(channelName, new InMemoryChannel(channelName, interceptorRegistry, objectMapper, eventPublisher));
        return channels.get(channelName);
    }

}
