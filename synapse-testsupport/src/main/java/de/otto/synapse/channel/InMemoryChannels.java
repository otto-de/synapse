package de.otto.synapse.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.ApplicationEventPublisher;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryChannels {

    private final ConcurrentMap<String,InMemoryChannel> channels = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;

    public InMemoryChannels(final ObjectMapper objectMapper,
                            final ApplicationEventPublisher eventPublisher) {
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
    }

    public InMemoryChannel getChannel(final String channelName) {
        channels.putIfAbsent(channelName, new InMemoryChannel(channelName, objectMapper, eventPublisher));
        return channels.get(channelName);
    }

}
