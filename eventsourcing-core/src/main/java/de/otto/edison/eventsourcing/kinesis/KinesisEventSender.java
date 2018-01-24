package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KinesisEventSender {
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0);

    private final KinesisStream kinesisStream;
    private final ObjectMapper objectMapper;
    private final TextEncryptor textEncryptor;

    public KinesisEventSender(KinesisStream kinesisStream, ObjectMapper objectMapper, TextEncryptor textEncryptor) {
        this.kinesisStream = kinesisStream;
        this.objectMapper = objectMapper;
        this.textEncryptor = textEncryptor;
    }

    public void sendEvent(String key, Object payload) throws JsonProcessingException {
        sendEvent(key, payload, true);
    }

    public void sendEvent(String key, Object payload, boolean encryptEvent) throws JsonProcessingException {
        if (encryptEvent) {
            kinesisStream.send(key, convertToEncryptedByteBuffer(payload));
        } else {
            kinesisStream.send(key, convertToByteBuffer(payload));
        }
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

        kinesisStream.sendMultiple(resultMap);
    }

    private ByteBuffer convertToByteBuffer(Object payload) throws JsonProcessingException {
        if (payload == null) {
            return EMPTY_BYTE_BUFFER;
        } else {
            return ByteBuffer.wrap(objectMapper.writeValueAsString(payload)
                    .getBytes(Charsets.UTF_8));
        }
    }

    private ByteBuffer convertToEncryptedByteBuffer(Object payload) throws JsonProcessingException {
        if (payload == null) {
            return EMPTY_BYTE_BUFFER;
        } else {
            return ByteBuffer.wrap(textEncryptor
                    .encrypt(objectMapper.writeValueAsString(payload))
                    .getBytes(Charsets.UTF_8));
        }
    }
}
