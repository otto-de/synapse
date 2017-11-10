package de.otto.edison.eventsourcing;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.kinesis.KinesisStream;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotService;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.util.function.Consumer;
import java.util.function.Predicate;

public class CompactingKinesisEventSource<T> implements EventSource<T> {

    @Autowired
    private SnapshotService snapshotService;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KinesisClient kinesisClient;

    private final String name;
    private final Class<T> payloadType;

    public CompactingKinesisEventSource(final String name,
                                        final Class<T> payloadType) {
        this.name = name;
        this.payloadType = payloadType;
    }

    public CompactingKinesisEventSource(final String name,
                                        final Class<T> payloadType,
                                        final SnapshotService snapshotService) {
        this.name = name;
        this.payloadType = payloadType;
        this.snapshotService = snapshotService;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<T>> stopCondition, Consumer<Event<T>> consumer) {
        final SnapshotEventSource<T> snapshotEventSource = new SnapshotEventSource<>(name, snapshotService, payloadType);

        KinesisStream kinesisStream = new KinesisStream(kinesisClient, name);
        final KinesisEventSource<T> kinesisEventSource = new KinesisEventSource<>(payloadType, objectMapper, kinesisStream);

        final StreamPosition streamPosition = snapshotEventSource.consumeAll(stopCondition, consumer);
        return kinesisEventSource.consumeAll(streamPosition, stopCondition, consumer);
    }
}
