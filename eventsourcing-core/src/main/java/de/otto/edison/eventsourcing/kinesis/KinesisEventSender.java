package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import java.nio.ByteBuffer;

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
        String jsonData = objectMapper.writeValueAsString(payload);
        ByteBuffer byteBuffer = convertToEncryptedByteBuffer(jsonData);
        kinesisStream.send(key, byteBuffer);
    }

    private ByteBuffer convertToEncryptedByteBuffer(String data) {
        return ByteBuffer.wrap(textEncryptor
                .encrypt(data)
                .getBytes(Charsets.UTF_8));
    }


}
