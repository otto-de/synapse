package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSender;
import de.otto.edison.eventsourcing.EventSenderFactory;
import org.springframework.security.crypto.encrypt.TextEncryptor;

public class InMemoryEventSenderFactory implements EventSenderFactory {

    private final ObjectMapper objectMapper;
    private final TextEncryptor textEncryptor;

    public InMemoryEventSenderFactory(ObjectMapper objectMapper, TextEncryptor textEncryptor) {
        this.objectMapper = objectMapper;
        this.textEncryptor = textEncryptor;
    }

    public EventSender createSenderForStream(String streamName) {
        return new InMemoryEventSender(streamName, objectMapper, textEncryptor, inMemoryStream);
    }

}
