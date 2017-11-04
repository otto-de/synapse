package de.otto.edison.eventsourcing;

import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.s3.SnapshotService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@Component
public class EventSourceFactory {

    private final SnapshotService snapshotService;
    private final KinesisClient kinesisClient;

    @Autowired
    public EventSourceFactory(final SnapshotService snapshotService,
                              final KinesisClient kinesisClient) {
        this.snapshotService = snapshotService;
        this.kinesisClient = kinesisClient;
    }

    public <T> EventSource<T> compactedEventSource(final String streamName,
                                                   final Class<T> payloadType) {
        return new CompactingKinesisEventSource<T>(streamName, payloadType, snapshotService, kinesisClient);
    }
}
