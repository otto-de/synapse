package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.MessageSender;

import static de.otto.edison.eventsourcing.message.Message.message;

public class InMemoryMessageSender implements MessageSender {

    private final ObjectMapper objectMapper;
    private final InMemoryStream eventStream;

    public InMemoryMessageSender(ObjectMapper objectMapper,
                                 InMemoryStream eventStream) {
        this.objectMapper = objectMapper;
        this.eventStream = eventStream;
    }

    @Override
    public <T> void send(String key, T payload) {
        try {
            eventStream.send(message(key, objectMapper.writeValueAsString(payload)));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
