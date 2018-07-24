package de.otto.synapse.messagequeue;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannel;
import org.springframework.context.ApplicationEventPublisher;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryQueueChannels {

    private final ConcurrentMap<String,InMemoryQueueChannel> channels = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;

    public InMemoryQueueChannels(final ObjectMapper objectMapper, final ApplicationEventPublisher eventPublisher) {
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
    }

    public InMemoryQueueChannel getChannel(final String channelName) {
        channels.putIfAbsent(channelName, new InMemoryQueueChannel(channelName, objectMapper, eventPublisher));
        return channels.get(channelName);
    }

}
