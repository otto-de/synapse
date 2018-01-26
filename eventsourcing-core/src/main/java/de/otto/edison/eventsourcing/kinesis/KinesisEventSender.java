package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import de.otto.edison.eventsourcing.EventSender;
import de.otto.edison.eventsourcing.inmemory.Tuple;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class KinesisEventSender implements EventSender {
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0);

    private final KinesisStream kinesisStream;
    private final ObjectMapper objectMapper;

    public KinesisEventSender(KinesisStream kinesisStream, ObjectMapper objectMapper) {
        this.kinesisStream = kinesisStream;
        this.objectMapper = objectMapper;
    }

    public void sendEvent(String key, Object payload) {
        kinesisStream.send(key, convertToByteBuffer(payload));
    }

    public void sendEvents(List<Tuple<String, Object>> events) {
        kinesisStream.sendBatch(events.stream()
                .map(e -> new Tuple<>(e.getFirst(), convertToByteBuffer(e.getSecond()))));
    }

    private ByteBuffer convertToByteBuffer(Object payload) {
        if (payload == null) {
            return EMPTY_BYTE_BUFFER;
        } else {
            try {
                return ByteBuffer.wrap(objectMapper.writeValueAsString(payload)
                        .getBytes(Charsets.UTF_8));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e.getMessage());
            }
        }
    }

}
