package de.otto.synapse.eventsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.consumer.EventSourceNotification;
import de.otto.synapse.message.Message;
import org.springframework.context.ApplicationEventPublisher;

import java.util.function.Predicate;

import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.time.Instant.now;

public class InMemoryEventSource extends AbstractEventSource {


    private final InMemoryChannel inMemoryChannel;

    public InMemoryEventSource(final String name,
                               final String streamName,
                               final InMemoryChannel inMemoryChannel,
                               final ApplicationEventPublisher eventPublisher,
                               final ObjectMapper objectMapper) {
        super(name, streamName, eventPublisher, objectMapper);
        this.inMemoryChannel = inMemoryChannel;
    }

    @Override
    public ChannelPosition consumeAll(final ChannelPosition startFrom,
                                      final Predicate<Message<?>> stopCondition) {
        publishEvent(startFrom, EventSourceNotification.Status.STARTED);
        boolean shouldStop;
        do {
            final Message<String> receivedMessage = inMemoryChannel.receive();

            if (receivedMessage == null) {
                return null;
            }

            final Message<String> messageWithHeaders = message(receivedMessage.getKey(), responseHeader(null, now()), receivedMessage.getPayload());
            dispatchingMessageConsumer().accept(messageWithHeaders);
            shouldStop = stopCondition.test(receivedMessage);
        } while (!shouldStop);
        publishEvent(null, EventSourceNotification.Status.FINISHED);
        return null;
    }
}
