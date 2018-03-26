package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.Status;
import de.otto.synapse.message.Message;
import org.springframework.context.ApplicationEventPublisher;

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
    public ChannelPosition consume(final ChannelPosition startFrom,
                                   final Predicate<Message<?>> stopCondition) {
        publishEvent(startFrom, EventSourceNotification.Status.STARTED);
        final ChannelPosition currentPosition = inMemoryChannel.consume(startFrom, stopCondition);
        publishEvent(currentPosition, EventSourceNotification.Status.FINISHED);
        return currentPosition;
    }
}
