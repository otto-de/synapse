package de.otto.edison.eventsourcing;

import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class CompactingKinesisEventSource<T> implements EventSource<T> {

    private final SnapshotEventSource snapshotEventSource;
    private final KinesisEventSource kinesisEventSource;
    private final String streamName;

    public CompactingKinesisEventSource(SnapshotEventSource snapshotEventSource,
                                        KinesisEventSource kinesisEventSource) {
        Objects.requireNonNull(snapshotEventSource, "snapshot event source must not be null");
        Objects.requireNonNull(kinesisEventSource, "kinesis event source must not be null");
        if (!snapshotEventSource.getStreamName().equals(kinesisEventSource.getStreamName())) {
            throw new IllegalArgumentException(String.format(
                    "given event sources must have same stream name, but was: '%s' and '%s'",
                    snapshotEventSource.getStreamName(), kinesisEventSource.getStreamName()));
        }
        this.snapshotEventSource = snapshotEventSource;
        this.kinesisEventSource = kinesisEventSource;
        this.streamName = kinesisEventSource.getStreamName();
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<T>> stopCondition, Consumer<Event<T>> consumer) {
        Predicate<Event> neverStop = e -> false;
        final StreamPosition streamPosition = snapshotEventSource.consumeAll(neverStop, consumer);
        return kinesisEventSource.consumeAll(streamPosition, stopCondition, consumer);
    }
}
