package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.AbstractEventSource;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotEventSource extends AbstractEventSource {

    private static final Logger LOG = getLogger(SnapshotEventSource.class);

    private final SnapshotReadService snapshotReadService;
    private final String streamName;
    private final SnapshotConsumerService snapshotConsumerService;
    private final ApplicationEventPublisher eventPublisher;

    private File forcedSnapshotFile = null;

    public SnapshotEventSource(final String name,
                               final String streamName,
                               final SnapshotReadService snapshotReadService,
                               final SnapshotConsumerService snapshotConsumerService,
                               final ApplicationEventPublisher eventPublisher,
                               final ObjectMapper objectMapper) {
        super(name, objectMapper);
        this.streamName = streamName;
        this.snapshotReadService = snapshotReadService;
        this.snapshotConsumerService = snapshotConsumerService;
        this.eventPublisher = eventPublisher;
    }

    public void setSnapshotFile(File file) {
        Objects.requireNonNull(file, "file must not be null");
        if (!file.exists() || !file.canRead()) {
            throw new IllegalArgumentException("snapshot file does not exists or is not readable");
        }
        this.forcedSnapshotFile = file;
    }

    public String getStreamName() {
        return streamName;
    }

    @Override
    public SnapshotStreamPosition consumeAll() {
        return consumeAll(StreamPosition.of(), event -> false);
    }

    @Override
    public SnapshotStreamPosition consumeAll(StreamPosition startFrom) {
        return consumeAll(StreamPosition.of());
    }

    @Override
    public SnapshotStreamPosition consumeAll(final StreamPosition startFrom,
                                     final Predicate<Event<?>> stopCondition) {
        SnapshotStreamPosition snapshotStreamPosition;

        try {
            publishEvent(startFrom, EventSourceNotification.Status.STARTED);

            Optional<File> snapshotFile = getSnapshotFileToConsume();
            if (snapshotFile.isPresent()) {
                StreamPosition streamPosition = snapshotConsumerService.consumeSnapshot(snapshotFile.get(), streamName, stopCondition, registeredConsumers());
                snapshotStreamPosition = SnapshotStreamPosition.of(streamPosition, SnapshotFileTimestampParser.getSnapshotTimestamp(snapshotFile.get().getName()));
            } else {
                snapshotStreamPosition = SnapshotStreamPosition.of();
            }
        } catch (Exception e) {
            publishEvent(SnapshotStreamPosition.of(), EventSourceNotification.Status.FAILED);
            throw new RuntimeException(e);
        } finally {
            LOG.info("Finished reading snapshot into Memory");
            cleanUpSnapshotFiles();
        }
        publishEvent(snapshotStreamPosition, EventSourceNotification.Status.FINISHED);
        return snapshotStreamPosition;
    }

   private Optional<File> getSnapshotFileToConsume() {
        if (this.forcedSnapshotFile == null) {
            return snapshotReadService.retrieveLatestSnapshot(streamName);
        } else {
            LOG.info("Use local Snapshot file: {}", forcedSnapshotFile);
            return Optional.of(forcedSnapshotFile);
        }
    }

    private void cleanUpSnapshotFiles() {
        if (this.forcedSnapshotFile == null) {
            snapshotReadService.deleteOlderSnapshots(streamName);
        }
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
