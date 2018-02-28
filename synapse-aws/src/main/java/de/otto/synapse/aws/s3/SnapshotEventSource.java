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
    private final String streamName;
    private final SnapshotConsumerService snapshotConsumerService;

    public SnapshotEventSource(final String name,
                               final String streamName,
                               final SnapshotReadService snapshotReadService,
                               final SnapshotConsumerService snapshotConsumerService,
                               final ApplicationEventPublisher eventPublisher,
                               final ObjectMapper objectMapper) {
        super(name, eventPublisher, objectMapper);
        this.streamName = streamName;
        this.snapshotReadService = snapshotReadService;
        this.snapshotConsumerService = snapshotConsumerService;
    }

    public String getStreamName() {
        return streamName;
    }

    @Override
    public SnapshotChannelPosition consumeAll() {
        return consumeAll(ChannelPosition.fromHorizon(), event -> false);
    }

    @Override
    public SnapshotChannelPosition consumeAll(ChannelPosition startFrom) {
        return consumeAll(ChannelPosition.fromHorizon());
    }

    @Override
    public SnapshotChannelPosition consumeAll(final ChannelPosition startFrom,
                                              final Predicate<Message<?>> stopCondition) {
        SnapshotChannelPosition snapshotStreamPosition;

        try {
            publishEvent(startFrom, EventSourceNotification.Status.STARTED, "Loading snapshot from S3.");

            Optional<File> snapshotFile = snapshotReadService.retrieveLatestSnapshot(streamName);
            if (snapshotFile.isPresent()) {
                ChannelPosition channelPosition = snapshotConsumerService.consumeSnapshot(snapshotFile.get(), streamName, stopCondition, dispatchingMessageConsumer());
                snapshotStreamPosition = SnapshotChannelPosition.of(channelPosition, SnapshotFileTimestampParser.getSnapshotTimestamp(snapshotFile.get().getName()));
            } else {
                snapshotStreamPosition = SnapshotChannelPosition.of();
            }
        } catch (RuntimeException e) {
            publishEvent(SnapshotChannelPosition.of(), EventSourceNotification.Status.FAILED, "Failed to load snapshot from S3: " + e.getMessage());
            throw e;
        } finally {
            LOG.info("Finished reading snapshot into Memory");
            snapshotReadService.deleteOlderSnapshots(streamName);
        }
        publishEvent(snapshotStreamPosition, EventSourceNotification.Status.FINISHED, "Finished to load snapshot from S3.");
        return snapshotStreamPosition;
    }

}
