package de.otto.synapse.aws.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.EventSourceNotification;
import de.otto.synapse.eventsource.AbstractEventSource;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.io.File;
import java.util.Optional;
import java.util.function.Predicate;

import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotEventSource extends AbstractEventSource {

    private static final Logger LOG = getLogger(SnapshotEventSource.class);

    private final SnapshotReadService snapshotReadService;
    private final SnapshotConsumerService snapshotConsumerService;

    public SnapshotEventSource(final String name,
                               final String streamName,
                               final SnapshotReadService snapshotReadService,
                               final SnapshotConsumerService snapshotConsumerService,
                               final ApplicationEventPublisher eventPublisher,
                               final ObjectMapper objectMapper) {
        super(name, streamName, eventPublisher, objectMapper);
        this.snapshotReadService = snapshotReadService;
        this.snapshotConsumerService = snapshotConsumerService;
    }

    @Override
    public SnapshotChannelPosition consumeAll(final ChannelPosition startFrom,
                                              final Predicate<Message<?>> stopCondition) {
        SnapshotChannelPosition snapshotStreamPosition;

        try {
            publishEvent(startFrom, EventSourceNotification.Status.STARTED, "Loading snapshot from S3.");

            Optional<File> snapshotFile = snapshotReadService.retrieveLatestSnapshot(getStreamName());
            if (snapshotFile.isPresent()) {
                ChannelPosition channelPosition = snapshotConsumerService.consumeSnapshot(snapshotFile.get(), getStreamName(), stopCondition, dispatchingMessageConsumer());
                snapshotStreamPosition = SnapshotChannelPosition.of(channelPosition, SnapshotFileTimestampParser.getSnapshotTimestamp(snapshotFile.get().getName()));
            } else {
                snapshotStreamPosition = SnapshotChannelPosition.of();
            }
        } catch (RuntimeException e) {
            publishEvent(SnapshotChannelPosition.of(), EventSourceNotification.Status.FAILED, "Failed to load snapshot from S3: " + e.getMessage());
            throw e;
        } finally {
            LOG.info("Finished reading snapshot into Memory");
            snapshotReadService.deleteOlderSnapshots(getStreamName());
        }
        publishEvent(snapshotStreamPosition, EventSourceNotification.Status.FINISHED, "Finished to load snapshot from S3.");
        return snapshotStreamPosition;
    }

}
