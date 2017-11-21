package de.otto.edison.eventsourcing.s3;

import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.slf4j.Logger;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotEventSource<T> implements EventSource<T> {

    private static final Logger LOG = getLogger(SnapshotEventSource.class);

    private final SnapshotReadService snapshotReadService;
    private final String streamName;
    private final SnapshotConsumerService snapshotConsumerService;
    private final Class<T> payloadType;

    public SnapshotEventSource(final String streamName,
                               final SnapshotReadService snapshotReadService,
                               final SnapshotConsumerService snapshotConsumerService,
                               final Class<T> payloadType) {
        this.streamName = streamName;
        this.snapshotReadService = snapshotReadService;
        this.snapshotConsumerService = snapshotConsumerService;
        this.payloadType = payloadType;
    }

    public String getStreamName() {
        return streamName;
    }

    @Override
    public SnapshotStreamPosition consumeAll(Consumer<Event<T>> consumer) {
        return consumeAll(StreamPosition.of(), event -> false, consumer);
    }

    @Override
    public SnapshotStreamPosition consumeAll(StreamPosition startFrom, Consumer<Event<T>> consumer) {
        return consumeAll(StreamPosition.of(), consumer);
    }

    @Override
    public SnapshotStreamPosition consumeAll(final StreamPosition startFrom,
                                     final Predicate<Event<T>> stopCondition,
                                     final Consumer<Event<T>> consumer) {
        // TODO: startFrom is ignored. the source should ignore / drop all events until startFrom is reached.

        try {
            Optional<File> latestSnapshot = snapshotReadService.downloadLatestSnapshot(this);
            LOG.info("Downloaded Snapshot");
            if (latestSnapshot.isPresent()) {
                StreamPosition streamPosition = snapshotConsumerService.consumeSnapshot(latestSnapshot.get(), streamName, stopCondition, consumer, payloadType);
                return SnapshotStreamPosition.of(streamPosition, SnapshotFileTimestampParser.getSnapshotTimestamp(latestSnapshot.get().getName()));
            } else {
                return SnapshotStreamPosition.of();
            }
        } catch (IOException | S3Exception e) {
            LOG.warn("Unable to load snapshot: {}", e.getMessage());
            throw new RuntimeException(e);
        } finally {
            LOG.info("Finished reading snapshot into Memory");
            snapshotReadService.deleteOlderSnapshots(streamName);
        }
    }

}
