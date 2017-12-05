package de.otto.edison.eventsourcing.s3;

import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.io.File;
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

    private ApplicationEventPublisher eventPublisher;

    public SnapshotEventSource(final String streamName,
                               final SnapshotReadService snapshotReadService,
                               final SnapshotConsumerService snapshotConsumerService,
                               final Class<T> payloadType,
                               final ApplicationEventPublisher eventPublisher) {
        this.streamName = streamName;
        this.snapshotReadService = snapshotReadService;
        this.snapshotConsumerService = snapshotConsumerService;
        this.payloadType = payloadType;
        this.eventPublisher = eventPublisher;
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
        SnapshotStreamPosition snapshotStreamPosition;

        try {
            publishEvent(startFrom, EventSourceNotification.Status.STARTED);

            Optional<File> latestSnapshot = snapshotReadService.retrieveLatestSnapshot(streamName);
            LOG.info("Downloaded Snapshot");
            if (latestSnapshot.isPresent()) {
                StreamPosition streamPosition = snapshotConsumerService.consumeSnapshot(latestSnapshot.get(), streamName, stopCondition, consumer, payloadType);
                snapshotStreamPosition = SnapshotStreamPosition.of(streamPosition, SnapshotFileTimestampParser.getSnapshotTimestamp(latestSnapshot.get().getName()));
            } else {
                snapshotStreamPosition = SnapshotStreamPosition.of();
            }
        } catch (Exception e) {
            publishEvent(SnapshotStreamPosition.of(), EventSourceNotification.Status.FAILED);
            throw new RuntimeException(e);
        } finally {
            LOG.info("Finished reading snapshot into Memory");
            snapshotReadService.deleteOlderSnapshots(streamName);
        }
        publishEvent(snapshotStreamPosition, EventSourceNotification.Status.FINISHED);
        return snapshotStreamPosition;
    }

    private void publishEvent(StreamPosition streamPosition, EventSourceNotification.Status status) {
        if (eventPublisher != null) {
            EventSourceNotification notification = EventSourceNotification.builder()
                    .withEventSource(this)
                    .withStreamPosition(streamPosition)
                    .withStatus(status)
                    .build();
            try {
                eventPublisher.publishEvent(notification);
            } catch (Exception e) {
                LOG.error("error publishing event source notification: {}",  notification, e);
            }
        }
    }

}
