package de.otto.edison.eventsourcing.kinesis;

import de.otto.edison.eventsourcing.consumer.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@Service
public class KinesisService {

    private KinesisClient kinesisClient;

    @Autowired
    public KinesisService(final KinesisClient kinesisClient) {
        this.kinesisClient = kinesisClient;
    }

    public <T> EventSource<T> getEventSource(final String streamName, final Class<T> payloadType) {
        return new KinesisEventSource<>(kinesisClient, streamName, payloadType);
    }

}
