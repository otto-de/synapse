package de.otto.synapse.inmemory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryChannels {

    private static final ConcurrentMap<String,InMemoryChannel> channels = new ConcurrentHashMap<>();

    public static InMemoryChannel getChannel(final String channelName) {
        channels.putIfAbsent(channelName, new InMemoryChannel());
        return channels.get(channelName);
    }

}
