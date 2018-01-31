package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import de.otto.edison.eventsourcing.EventSender;
import de.otto.edison.eventsourcing.event.EventBody;

import java.nio.ByteBuffer;
import java.util.List;

import static de.otto.edison.eventsourcing.event.EventBody.eventBody;

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

    public void sendEvents(List<EventBody<Object>> events) {
        kinesisStream.sendBatch(events.stream()
                .map(e -> eventBody(e.getKey(), convertToByteBuffer(e.getPayload()))));
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
