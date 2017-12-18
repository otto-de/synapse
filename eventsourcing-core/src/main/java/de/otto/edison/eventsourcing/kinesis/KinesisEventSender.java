package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KinesisEventSender {

    private final KinesisStream kinesisStream;
    private final ObjectMapper objectMapper;
    private final TextEncryptor textEncryptor;

    public KinesisEventSender(KinesisStream kinesisStream, ObjectMapper objectMapper, TextEncryptor textEncryptor) {
        this.kinesisStream = kinesisStream;
        this.objectMapper = objectMapper;
        this.textEncryptor = textEncryptor;
    }

    public void sendEvent(String key, Object payload) throws JsonProcessingException {
        kinesisStream.send(key, convertToEncryptedByteBuffer(payload));
    }

    public void sendEvents(Map<String, Object> events) throws JsonProcessingException {
        Map<String, ByteBuffer> resultMap = new HashMap<>(events.size());
        for (Map.Entry<String, Object> event : events.entrySet()) {
            resultMap.put(event.getKey(), convertToEncryptedByteBuffer(event.getValue()));
        }

        kinesisStream.sendMultiple(resultMap);
    }

    private ByteBuffer convertToEncryptedByteBuffer(Object payload) throws JsonProcessingException {
        return ByteBuffer.wrap(textEncryptor
                .encrypt(objectMapper.writeValueAsString(payload))
                .getBytes(Charsets.UTF_8));
    }
}
