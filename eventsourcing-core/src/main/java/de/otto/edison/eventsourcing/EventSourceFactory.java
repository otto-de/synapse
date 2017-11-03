package de.otto.edison.eventsourcing;

import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.util.function.Predicate;

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

    public <T> EventSource<T> compactedEventSource(String streamName, Class<T> payloadType) {
        final SnapshotEventSource<T> snapshotEventSource =
                new SnapshotEventSource<>(streamName, snapshotService, payloadType);

        final KinesisEventSource<T> kinesisEventSource =
                new KinesisEventSource<>(kinesisClient, streamName, payloadType);

        return new EventSource<T>() {
            @Override
            public String name() {
                return streamName;
            }

            @Override
            public StreamPosition consumeAll(final StreamPosition startFrom,
                                             final Predicate<Event<T>> stopCondition,
                                             final EventConsumer<T> consumer) {
                final StreamPosition streamPosition = snapshotEventSource.consumeAll(consumer);
                return kinesisEventSource.consumeAll(streamPosition, consumer);
            }
        };
    }
}
