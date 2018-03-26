package de.otto.synapse.channel;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryChannels {

    private final ConcurrentMap<String,InMemoryChannel> channels = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;

    public InMemoryChannels(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public InMemoryChannel getChannel(final String channelName) {
        channels.putIfAbsent(channelName, new InMemoryChannel(channelName, objectMapper));
        return channels.get(channelName);
    }

}
