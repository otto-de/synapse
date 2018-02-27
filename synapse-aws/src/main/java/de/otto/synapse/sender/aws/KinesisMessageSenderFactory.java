package de.otto.synapse.sender.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.sender.MessageSender;
import de.otto.synapse.sender.MessageSenderFactory;
import de.otto.synapse.translator.JsonByteBufferMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.nio.ByteBuffer;

public class KinesisMessageSenderFactory implements MessageSenderFactory {

    private final MessageTranslator<ByteBuffer> messageTranslator;
    private final KinesisClient kinesisClient;

    public KinesisMessageSenderFactory(final ObjectMapper objectMapper,
                                       final KinesisClient kinesisClient) {
        this.messageTranslator = new JsonByteBufferMessageTranslator(objectMapper);
        this.kinesisClient = kinesisClient;
    }

    public MessageSender createSenderForStream(final String streamName) {
        return new KinesisMessageSender(streamName, messageTranslator, kinesisClient);
    }

}
