package de.otto.edison.eventsourcing;

import de.otto.edison.eventsourcing.consumer.*;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;

import java.io.File;
import java.util.Objects;
import java.util.function.Predicate;

public class CompactingKinesisEventSource implements EventSource {

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

    public void setSnapshotFile(File file) {
        snapshotEventSource.setSnapshotFile(file);
    }

    /**
     * Registers a new EventConsumer at the EventSource.
     * <p>
     * {@link EventConsumer consumers} have to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param eventConsumer
     */
    @Override
    public void register(final EventConsumer<?> eventConsumer) {
        snapshotEventSource.register(eventConsumer);
        kinesisEventSource.register(eventConsumer);
    }

    /**
     * Returns the list of registered EventConsumers.
     *
     * @return list of registered EventConsumers
     */
    @Override
    public EventConsumers registeredConsumers() {
        return snapshotEventSource.registeredConsumers();
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<?>> stopCondition) {
        Predicate<Event<?>> neverStop = e -> false;
        final StreamPosition streamPosition = snapshotEventSource.consumeAll(neverStop);
        return kinesisEventSource.consumeAll(streamPosition, stopCondition);
    }
}
