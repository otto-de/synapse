package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.InMemoryChannel;

import java.time.Instant;

public class InMemoryEventSource extends AbstractEventSource {


    private final InMemoryChannel inMemoryChannel;

    public InMemoryEventSource(final String name,
                               final InMemoryChannel inMemoryChannel) {
        super(name, inMemoryChannel);
        this.inMemoryChannel = inMemoryChannel;
    }

    @Override
    public ChannelPosition consumeUntil(final ChannelPosition startFrom,
                                        final Instant until) {
        return inMemoryChannel.consumeUntil(startFrom, until);
    }
}
