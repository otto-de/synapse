package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSender;

import static de.otto.edison.eventsourcing.event.EventBody.eventBody;

public class InMemoryEventSender implements EventSender {

    private final String name;
    private final ObjectMapper objectMapper;
    private final InMemoryStream eventStream;

    public InMemoryEventSender(String name,
                               ObjectMapper objectMapper,
                               InMemoryStream eventStream) {
        this.name = name;
        this.objectMapper = objectMapper;
        this.eventStream = eventStream;
    }

    @Override
    public void sendEvent(String key, Object payload) {
        try {
            eventStream.send(eventBody(key, objectMapper.writeValueAsString(payload)));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
