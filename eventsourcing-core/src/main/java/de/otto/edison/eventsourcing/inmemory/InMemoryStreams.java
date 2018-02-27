package de.otto.edison.eventsourcing.inmemory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryStreams {

    private static final ConcurrentMap<String,InMemoryStream> channels = new ConcurrentHashMap<>();

    public static InMemoryStream getChannel(final String channelName) {
        channels.putIfAbsent(channelName, new InMemoryStream());
        return channels.get(channelName);
    }

}
