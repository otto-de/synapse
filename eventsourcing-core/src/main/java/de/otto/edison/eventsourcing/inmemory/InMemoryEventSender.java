package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import de.otto.edison.eventsourcing.EventSender;

import java.nio.ByteBuffer;

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
            eventStream.send(new Tuple<>(key, objectMapper.writeValueAsString(payload)));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
