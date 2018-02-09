package de.otto.edison.eventsourcing.aws.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import de.otto.edison.eventsourcing.MessageSender;
import de.otto.edison.eventsourcing.message.Message;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static de.otto.edison.eventsourcing.message.ByteBufferMessage.byteBufferMessage;
import static de.otto.edison.eventsourcing.message.Message.message;

public class KinesisMessageSender implements MessageSender {
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0);

    private final KinesisStream kinesisStream;
    private final ObjectMapper objectMapper;

    public KinesisMessageSender(KinesisStream kinesisStream, ObjectMapper objectMapper) {
        this.kinesisStream = kinesisStream;
        this.objectMapper = objectMapper;
    }

    @Override
    public <T> void send(Message<T> message) {
        kinesisStream.send(
                byteBufferMessage(message.getKey(), convertToByteBuffer(message.getPayload()))
        );
    }

    @Override
    public <T> void sendBatch(Stream<Message<T>> events) {
        kinesisStream.sendBatch(events
                .map(e -> message(e.getKey(), convertToByteBuffer(e.getPayload()))));
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
