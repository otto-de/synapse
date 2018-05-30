package de.otto.synapse.compaction.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.SnapshotReaderNotification;
import de.otto.synapse.info.SnapshotReaderStatus;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.io.File;
import java.time.Instant;
import java.util.Optional;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.info.SnapshotReaderStatus.*;
import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotEventSource implements EventSource {

    private static final Logger LOG = getLogger(SnapshotEventSource.class);

    private final String name;
    private final String channelName;
    private final SnapshotReadService snapshotReadService;
    private final ApplicationEventPublisher eventPublisher;
    private final MessageDispatcher messageDispatcher;

    public SnapshotEventSource(final String name,
                               final String channelName,
                               final SnapshotReadService snapshotReadService,
                               final ApplicationEventPublisher eventPublisher,
                               final ObjectMapper objectMapper) {
        this.name = name;
        this.channelName = channelName;
        this.snapshotReadService = snapshotReadService;
        this.eventPublisher = eventPublisher;
        this.messageDispatcher = new MessageDispatcher(objectMapper);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void register(MessageConsumer<?> messageConsumer) {
        messageDispatcher.add(messageConsumer);
    }

    @Nonnull
    @Override
    public MessageDispatcher getMessageDispatcher() {
        return messageDispatcher;
    }

    @Nonnull
    @Override
    public MessageLogReceiverEndpoint getMessageLogReceiverEndpoint() {
        // TODO
        return null;
    }

    @Override
    public String getChannelName() {
        return channelName;
    }

    @Nonnull
    @Override
    public ChannelPosition consumeUntil(@Nonnull final ChannelPosition startFrom,
                                        @Nonnull final Instant until) {
        ChannelPosition snapshotStreamPosition;
        Instant snapshotTimestamp = null;

        try {
            publishEvent(STARTING, "Retrieve snapshot file from S3.", null);

            Optional<File> snapshotFile = snapshotReadService.retrieveLatestSnapshot(this.getChannelName());
            if (snapshotFile.isPresent()) {
                snapshotTimestamp = SnapshotFileHelper.getSnapshotTimestamp(snapshotFile.get().getName());
                publishEvent(STARTED, "Retrieve snapshot file from S3.", snapshotTimestamp);
                snapshotStreamPosition = new SnapshotParser().parse(snapshotFile.get(), getMessageDispatcher());
            } else {
                snapshotStreamPosition = fromHorizon();
            }
        } catch (RuntimeException e) {
            publishEvent(FAILED, "Failed to load snapshot from S3: " + e.getMessage(), snapshotTimestamp);
            throw e;
        } finally {
            LOG.info("Finished reading snapshot into Memory");
        }

        publishEvent(FINISHED, "Finished to load snapshot from S3.", snapshotTimestamp);
        return snapshotStreamPosition;
    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isStopping() {
        return false;
    }

    private void publishEvent(SnapshotReaderStatus status, String message, Instant snapshotTimestamp) {
        if (eventPublisher != null) {
            SnapshotReaderNotification notification = SnapshotReaderNotification.builder()
                    .withSnapshotTimestamp(snapshotTimestamp)
                    .withChannelName(this.getChannelName())
                    .withStatus(status)
                    .withMessage(message)
                    .build();
            try {
                eventPublisher.publishEvent(notification);
            } catch (Exception e) {
                LOG.error("error publishing event source notification: {}", notification, e);
            }
        }
    }
}
