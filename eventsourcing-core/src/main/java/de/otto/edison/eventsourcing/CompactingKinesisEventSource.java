package de.otto.edison.eventsourcing;

import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotService;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.util.function.Predicate;

public class CompactingKinesisEventSource<T> implements EventSource<T> {

    @Autowired
    private SnapshotService snapshotService;
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
                                        final SnapshotService snapshotService,
                                        final KinesisClient kinesisClient) {
        this.name = name;
        this.payloadType = payloadType;
        this.snapshotService = snapshotService;
        this.kinesisClient = kinesisClient;
    }

    /**
     * Returns the name of the EventSource.
     * <p>
     * For streaming event-sources, this is the name of the event stream.
     * </p>
     *
     * @return name
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Consumes all events from the EventSource, beginning with {@link StreamPosition startFrom}, until
     * the {@link Predicate stopCondition} is met.
     * <p>
     * The {@link EventConsumer consumer} will be called zero or more times, depending on
     * the number of events retrieved from the EventSource.
     * </p>
     *
     * @param startFrom     the read position returned from earlier executions
     * @param stopCondition the predicate used as a stop condition
     * @param consumer      consumer used to process events
     * @return the new read position
     */
    @Override
    public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<T>> stopCondition, EventConsumer<T> consumer) {
        final SnapshotEventSource<T> snapshotEventSource = new SnapshotEventSource<>(name, snapshotService, payloadType);

        final KinesisEventSource<T> kinesisEventSource = new KinesisEventSource<>(kinesisClient, name, payloadType);

        final StreamPosition streamPosition = snapshotEventSource.consumeAll(stopCondition, consumer);
        return kinesisEventSource.consumeAll(streamPosition, stopCondition, consumer);
    }
}
