package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.AbstractEventSource;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.StreamPosition;

import java.util.function.Predicate;

public class InMemoryEventSource extends AbstractEventSource {

    private InMemoryStream eventStream;

    public InMemoryEventSource(String name, InMemoryStream inMemoryStream, ObjectMapper objectMapper) {
        super(name, objectMapper);
    }

    @Override
    public String getStreamName() {
        return null;
    }

    @Override
    public StreamPosition consumeAll() {
        return null;
    }

    @Override
    public StreamPosition consumeAll(StreamPosition startFrom) {
        return null;
    }

    @Override
    public StreamPosition consumeAll(Predicate<Event<?>> stopCondition) {
        return null;
    }

    @Override
    public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<?>> stopCondition) {
        return null;
    }
}
