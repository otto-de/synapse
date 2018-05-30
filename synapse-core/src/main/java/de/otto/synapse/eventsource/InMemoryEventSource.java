package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.InMemoryChannel;

import javax.annotation.Nonnull;
import java.time.Instant;

public class InMemoryEventSource extends AbstractEventSource {


    private final InMemoryChannel inMemoryChannel;

    public InMemoryEventSource(final String name,
                               final InMemoryChannel inMemoryChannel) {
        super(name, inMemoryChannel);
        this.inMemoryChannel = inMemoryChannel;
    }

    @Nonnull
    @Override
    public ChannelPosition consumeUntil(@Nonnull final ChannelPosition startFrom,
                                        @Nonnull final Instant until) {
        return inMemoryChannel.consumeUntil(startFrom, until);
    }
}
