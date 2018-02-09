package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.AbstractEventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.event.Event;
import de.otto.edison.eventsourcing.event.EventBody;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Instant;
import java.util.function.Predicate;

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
    public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<?>> stopCondition) {
        publishEvent(startFrom, EventSourceNotification.Status.STARTED);
        boolean shouldStop;
        do {
            EventBody<String> eventBody = inMemoryStream.receive();

            if (eventBody == null) {
                return null;
            }

            Event<String> event = Event.event(eventBody, "0", Instant.now());

            registeredConsumers().encodeAndSend(event);
            shouldStop = stopCondition.test(event);
        } while (!shouldStop);
        publishEvent(null, EventSourceNotification.Status.FINISHED);
        return null;
    }
}
