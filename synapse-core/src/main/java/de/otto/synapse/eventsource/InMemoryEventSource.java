package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.info.MessageEndpointStatus;
import de.otto.synapse.message.Message;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Instant;
import java.util.function.Predicate;

public class InMemoryEventSource extends AbstractEventSource {


    private final InMemoryChannel inMemoryChannel;

    public InMemoryEventSource(final String name,
                               final InMemoryChannel inMemoryChannel,
                               final ApplicationEventPublisher eventPublisher) {
        super(name, inMemoryChannel, eventPublisher);
        this.inMemoryChannel = inMemoryChannel;
    }

    @Override
    public ChannelPosition consumeUntil(final ChannelPosition startFrom,
                                        final Instant until) {
        publishEvent(startFrom, MessageEndpointStatus.STARTING);
        final ChannelPosition currentPosition = inMemoryChannel.consumeUntil(startFrom, until);
        publishEvent(currentPosition, MessageEndpointStatus.FINISHED);
        return currentPosition;
    }
}
