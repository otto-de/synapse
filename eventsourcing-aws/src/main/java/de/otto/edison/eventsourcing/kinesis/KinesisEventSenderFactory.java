package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSender;
import de.otto.edison.eventsourcing.EventSenderFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;

public class KinesisEventSenderFactory implements EventSenderFactory {

    private final ObjectMapper objectMapper;
    private final KinesisClient kinesisClient;

    public KinesisEventSenderFactory(ObjectMapper objectMapper, KinesisClient kinesisClient) {
        this.objectMapper = objectMapper;
        this.kinesisClient = kinesisClient;
    }

    public EventSender createSenderForStream(String streamName) {
        KinesisStream kinesisStream = new KinesisStream(kinesisClient, streamName);
        return new KinesisEventSender(kinesisStream, objectMapper);
    }

}
