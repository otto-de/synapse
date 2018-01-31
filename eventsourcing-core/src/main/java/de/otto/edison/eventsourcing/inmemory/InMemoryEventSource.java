package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.AbstractEventSource;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.StreamPosition;

import java.time.Instant;
import java.util.function.Predicate;

public class InMemoryEventSource extends AbstractEventSource {


    private final InMemoryStream inMemoryStream;

    public InMemoryEventSource(final String name,
                               final InMemoryStream inMemoryStream,
                               final ObjectMapper objectMapper) {
        super(name, objectMapper);
        this.inMemoryStream = inMemoryStream;
    }

    @Override
    public String getStreamName() {
        return getName();
    }

    // TODO: move to base
    @Override
    public StreamPosition consumeAll() {
        return consumeAll(StreamPosition.of(), e -> true);
    }

    // TODO: move to base
    @Override
    public StreamPosition consumeAll(StreamPosition startFrom) {
        return consumeAll(startFrom, e -> true);
    }

    // TODO: move to base
    @Override
    public StreamPosition consumeAll(Predicate<Event<?>> stopCondition) {
        return consumeAll(StreamPosition.of(), stopCondition);
    }

    @Override
    public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<?>> stopCondition) {
        boolean shouldStop;
        do {
            Tuple<String, String> tuple = inMemoryStream.receive();
            Event<String> event = Event.event(tuple.getFirst(), tuple.getSecond(), "0", Instant.now());

            registeredConsumers().encodeAndSend(event);

            shouldStop = stopCondition.test(event);
        } while (!shouldStop);
        return null;
    }
}
