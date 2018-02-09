package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.MessageSender;
import de.otto.edison.eventsourcing.message.Message;

import static de.otto.edison.eventsourcing.message.Message.message;
import static de.otto.edison.eventsourcing.message.StringMessage.stringMessage;

public class InMemoryMessageSender implements MessageSender {

    private final ObjectMapper objectMapper;
    private final InMemoryStream eventStream;

    public InMemoryMessageSender(ObjectMapper objectMapper,
                                 InMemoryStream eventStream) {
        this.objectMapper = objectMapper;
        this.eventStream = eventStream;
    }

    @Override
    public <T> void send(Message<T> message) {
        try {
            eventStream.send(
                    stringMessage(message.getKey(), objectMapper.writeValueAsString(message.getPayload()))
            );
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
