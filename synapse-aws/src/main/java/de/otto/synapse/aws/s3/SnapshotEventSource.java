package de.otto.synapse.aws.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.aws.SnapshotMessageEndpointNotification;
import de.otto.synapse.info.MessageEndpointNotification;
import de.otto.synapse.info.MessageEndpointStatus;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.io.File;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Predicate;

import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotEventSource implements EventSource {

    private static final Logger LOG = getLogger(SnapshotEventSource.class);

    private final String name;
    private final String channelName;
    private final SnapshotReadService snapshotReadService;
    private final SnapshotConsumerService snapshotConsumerService;
    private final ApplicationEventPublisher eventPublisher;
    private final MessageDispatcher messageDispatcher;

    public SnapshotEventSource(final String name,
                               final String channelName,
                               final SnapshotReadService snapshotReadService,
                               final SnapshotConsumerService snapshotConsumerService,
                               final ApplicationEventPublisher eventPublisher,
                               final ObjectMapper objectMapper) {
        this.name = name;
        this.channelName = channelName;
        this.snapshotReadService = snapshotReadService;
        this.snapshotConsumerService = snapshotConsumerService;
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

    @Override
    public String getChannelName() {
        return channelName;
    }

    @Override
    public ChannelPosition consume(final ChannelPosition startFrom,
                                   final Predicate<Message<?>> stopCondition) {
        ChannelPosition snapshotStreamPosition;
        Instant snapshotTimestamp = null;

        try {
            publishEvent(startFrom, MessageEndpointStatus.STARTING, "Retrieve snapshot file from S3.", null);

            Optional<File> snapshotFile = snapshotReadService.retrieveLatestSnapshot(this.getChannelName());
                publishEvent(startFrom, MessageEndpointStatus.STARTED, "Loading snapshot.", null);
            if (snapshotFile.isPresent()) {
                snapshotTimestamp = SnapshotFileTimestampParser.getSnapshotTimestamp(snapshotFile.get().getName());
                snapshotStreamPosition = snapshotConsumerService.consumeSnapshot(snapshotFile.get(), stopCondition, getMessageDispatcher());
            } else {
                snapshotStreamPosition = ChannelPosition.fromHorizon();
            }
        } catch (RuntimeException e) {
            publishEvent(ChannelPosition.fromHorizon(), MessageEndpointStatus.FAILED, "Failed to load snapshot from S3: " + e.getMessage(), null);
            throw e;
        } finally {
            LOG.info("Finished reading snapshot into Memory");
            snapshotReadService.deleteOlderSnapshots(this.getChannelName());
        }

        publishEvent(snapshotStreamPosition, MessageEndpointStatus.FINISHED, "Finished to load snapshot from S3.", snapshotTimestamp);
        return snapshotStreamPosition;
    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isStopping() {
        return false;
    }

    private void publishEvent(ChannelPosition channelPosition, MessageEndpointStatus status, String message, Instant snapshotTimestamp) {
        if (eventPublisher != null) {
            MessageEndpointNotification notification = SnapshotMessageEndpointNotification.builder()
                    .withSnapshotTimestamp(snapshotTimestamp)
                    .withChannelName(this.getChannelName())
                    .withChannelPosition(channelPosition)
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
