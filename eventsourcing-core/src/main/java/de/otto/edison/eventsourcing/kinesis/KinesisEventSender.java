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

    public <T> void sendEvent(String key, T payload) throws JsonProcessingException {
        kinesisStream.send(key, convertToEncryptedByteBuffer(payload));
    }

    public <T> void sendEvents(Map<String, T> events) throws JsonProcessingException {
        Map<String, ByteBuffer> resultMap = new HashMap<>(events.size());
        for (Map.Entry<String, T> event : events.entrySet()) {
            resultMap.put(event.getKey(), convertToEncryptedByteBuffer(event.getValue()));
        }

        kinesisStream.sendMultiple(resultMap);
    }

    private <T> ByteBuffer convertToEncryptedByteBuffer(T payload) throws JsonProcessingException {
        return ByteBuffer.wrap(textEncryptor
                .encrypt(objectMapper.writeValueAsString(payload))
                .getBytes(Charsets.UTF_8));
    }
}
