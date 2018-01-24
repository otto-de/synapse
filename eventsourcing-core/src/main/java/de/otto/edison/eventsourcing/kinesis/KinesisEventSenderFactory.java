package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.kinesis.KinesisClient;

public class KinesisEventSenderFactory {

    private final ObjectMapper objectMapper;
    private final KinesisClient kinesisClient;

    public KinesisEventSenderFactory(ObjectMapper objectMapper, KinesisClient kinesisClient) {
        this.objectMapper = objectMapper;
        this.kinesisClient = kinesisClient;
    }

    public KinesisEventSender createSenderForStream(String streamName) {
        KinesisStream kinesisStream = new KinesisStream(kinesisClient, streamName);
        return new KinesisEventSender(kinesisStream, objectMapper);
    }

}
