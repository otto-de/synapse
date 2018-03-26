package de.otto.synapse.aws.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceNotification;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.io.File;
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

        try {
            publishEvent(startFrom, EventSourceNotification.Status.STARTED, "Loading snapshot from S3.");

            Optional<File> snapshotFile = snapshotReadService.retrieveLatestSnapshot(this.getChannelName());
            if (snapshotFile.isPresent()) {
                //Instant snapshotTimestamp = SnapshotFileTimestampParser.getSnapshotTimestamp(snapshotFile.get().getName());
                snapshotStreamPosition = snapshotConsumerService.consumeSnapshot(snapshotFile.get(), this.getChannelName(), stopCondition, getMessageDispatcher());
            } else {
                snapshotStreamPosition = ChannelPosition.fromHorizon();
            }
        } catch (RuntimeException e) {
            publishEvent(ChannelPosition.fromHorizon(), EventSourceNotification.Status.FAILED, "Failed to load snapshot from S3: " + e.getMessage());
            throw e;
        } finally {
            LOG.info("Finished reading snapshot into Memory");
            snapshotReadService.deleteOlderSnapshots(this.getChannelName());
        }
        publishEvent(snapshotStreamPosition, EventSourceNotification.Status.FINISHED, "Finished to load snapshot from S3.");
        return snapshotStreamPosition;
    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isStopping() {
        return false;
    }

    protected void publishEvent(ChannelPosition channelPosition, EventSourceNotification.Status status, String message) {
        if (eventPublisher != null) {
            EventSourceNotification notification = EventSourceNotification.builder()
                    .withEventSourceName(name)
                    .withStreamName(this.getChannelName())
                    .withStreamPosition(channelPosition)
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
