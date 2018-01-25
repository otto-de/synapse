package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import de.otto.edison.eventsourcing.EventSender;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KinesisEventSender implements EventSender {
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0);

    private final KinesisStream kinesisStream;
    private final ObjectMapper objectMapper;

    public KinesisEventSender(KinesisStream kinesisStream, ObjectMapper objectMapper) {
        this.kinesisStream = kinesisStream;
        this.objectMapper = objectMapper;
    }

    public void sendEvent(String key, Object payload) throws JsonProcessingException {
        kinesisStream.send(key, convertToByteBuffer(payload));
    }

    public void sendEvents(Map<String, Object> events) throws JsonProcessingException {
        Map<String, ByteBuffer> resultMap = new HashMap<>(events.size());
        for (Map.Entry<String, Object> event : events.entrySet()) {
            resultMap.put(event.getKey(), convertToByteBuffer(event.getValue()));
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

}
