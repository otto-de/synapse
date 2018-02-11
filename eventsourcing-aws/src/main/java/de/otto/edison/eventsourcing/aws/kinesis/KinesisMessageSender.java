package de.otto.edison.eventsourcing.aws.kinesis;

import de.otto.edison.eventsourcing.MessageSender;
import de.otto.edison.eventsourcing.message.Message;
import de.otto.edison.eventsourcing.translator.MessageTranslator;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class KinesisMessageSender implements MessageSender {

    private final KinesisStream kinesisStream;
    private final MessageTranslator<ByteBuffer> messageTranslator;

    public KinesisMessageSender(final KinesisStream kinesisStream,
                                final MessageTranslator<ByteBuffer> messageTranslator) {
        this.kinesisStream = kinesisStream;
        this.messageTranslator = messageTranslator;
    }

    @Override
    public <T> void send(Message<T> message) {
        kinesisStream.send(messageTranslator.translate(message));
    }

    @Override
    public <T> void sendBatch(Stream<Message<T>> messageStream) {
        kinesisStream.sendBatch(messageStream.map(messageTranslator::translate));
    }

}
