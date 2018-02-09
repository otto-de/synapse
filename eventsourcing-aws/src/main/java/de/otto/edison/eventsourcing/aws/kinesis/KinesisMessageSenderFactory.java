package de.otto.edison.eventsourcing.aws.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.MessageSender;
import de.otto.edison.eventsourcing.MessageSenderFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;

public class KinesisMessageSenderFactory implements MessageSenderFactory {

    private final ObjectMapper objectMapper;
    private final KinesisClient kinesisClient;

    public KinesisMessageSenderFactory(ObjectMapper objectMapper, KinesisClient kinesisClient) {
        this.objectMapper = objectMapper;
        this.kinesisClient = kinesisClient;
    }

    public MessageSender createSenderForStream(String streamName) {
        KinesisStream kinesisStream = new KinesisStream(kinesisClient, streamName);
        return new KinesisMessageSender(kinesisStream, objectMapper);
    }

}
