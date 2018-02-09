package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.AbstractEventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.event.Message;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Instant;
import java.util.function.Predicate;

import static de.otto.edison.eventsourcing.event.Header.responseHeader;
import static de.otto.edison.eventsourcing.event.Message.message;

public class InMemoryEventSource extends AbstractEventSource {


    private final InMemoryStream inMemoryStream;
    private final String streamName;

    public InMemoryEventSource(final String name,
                               final String streamName,
                               final InMemoryStream inMemoryStream,
                               final ApplicationEventPublisher eventPublisher,
                               final ObjectMapper objectMapper) {
        super(name, eventPublisher, objectMapper);
        this.streamName = streamName;
        this.inMemoryStream = inMemoryStream;
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Message<?>> stopCondition) {
        publishEvent(startFrom, EventSourceNotification.Status.STARTED);
        boolean shouldStop;
        do {
            Message<String> message = inMemoryStream.receive();

            if (message == null) {
                return null;
            }

            registeredConsumers().encodeAndSend(
                    message(message.getKey(), responseHeader("0", Instant.now()), message.getPayload())
            );
            shouldStop = stopCondition.test(message);
        } while (!shouldStop);
        publishEvent(null, EventSourceNotification.Status.FINISHED);
        return null;
    }
}
