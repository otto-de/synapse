package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import de.otto.edison.eventsourcing.EventSender;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class InMemoryEventSender implements EventSender {

    private final String name;
    private final ObjectMapper objectMapper;
    private final TextEncryptor textEncryptor;
    private final InMemoryStream inMemoryStream;

    public InMemoryEventSender(String name,
                               ObjectMapper objectMapper,
                               TextEncryptor textEncryptor,
                               InMemoryStream inMemoryStream) {
        this.name = name;
        this.objectMapper = objectMapper;
        this.textEncryptor = textEncryptor;
        this.inMemoryStream = inMemoryStream;
    }

    public void sendEvent(String key, Object payload) throws JsonProcessingException {
        sendEvent(key, payload, true);
    }

    public void sendEvent(String key, Object payload, boolean encryptEvent) throws JsonProcessingException {

    }

    public void sendEvents(Map<String, Object> events) throws JsonProcessingException {
        sendEvents(events, true);
    }

    public void sendEvents(Map<String, Object> events, boolean encryptEvents) throws JsonProcessingException {
        Map<String, ByteBuffer> resultMap = new HashMap<>(events.size());
        for (Map.Entry<String, Object> event : events.entrySet()) {
            if (encryptEvents) {
                resultMap.put(event.getKey(), convertToEncryptedByteBuffer(event.getValue()));
            } else {
                resultMap.put(event.getKey(), convertToByteBuffer(event.getValue()));
            }
        }

    }

    private ByteBuffer convertToByteBuffer(Object payload) throws JsonProcessingException {
        return ByteBuffer.wrap(objectMapper.writeValueAsString(payload)
                .getBytes(Charsets.UTF_8));
    }

    private ByteBuffer convertToEncryptedByteBuffer(Object payload) throws JsonProcessingException {
        return ByteBuffer.wrap(textEncryptor
                .encrypt(objectMapper.writeValueAsString(payload))
                .getBytes(Charsets.UTF_8));
    }
}
