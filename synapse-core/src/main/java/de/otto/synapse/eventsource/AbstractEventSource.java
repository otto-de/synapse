package de.otto.synapse.eventsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.slf4j.LoggerFactory.getLogger;

public abstract class AbstractEventSource implements EventSource {

    private static final Logger LOG = getLogger(AbstractEventSource.class);

    private final String name;
    private final String streamName;
    private final ApplicationEventPublisher eventPublisher;
    private final MessageDispatcher messageConsumer;
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    public AbstractEventSource(final String name,
                               final String streamName,
                               final ApplicationEventPublisher eventPublisher,
                               final ObjectMapper objectMapper) {
        this.name = name;
        this.streamName = streamName;
        this.eventPublisher = eventPublisher;
        this.messageConsumer = new MessageDispatcher(objectMapper);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public void stop() {
        LOG.info("Stopping EventSource {}", name);
        stopping.set(true);
    }

    @Override
    public boolean isStopping() {
        return stopping.get();
    }

    /**
     * Registers a new EventConsumer at the EventSource.
     * <p>
     * {@link MessageConsumer consumers} have to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param messageConsumer the EventConsumer that is registered at the EventSource
     */
    @Override
    public void register(final MessageConsumer<?> messageConsumer) {
        this.messageConsumer.add(messageConsumer);
    }

    /**
     * Returns the MessageDispatcher used to register {@link MessageConsumer consumers} at the EventSource.
     *
     * @return MessageDispatcher
     */
    public MessageDispatcher getMessageDispatcher() {
        return messageConsumer;
    }

    protected void publishEvent(ChannelPosition channelPosition, EventSourceNotification.Status status) {
        publishEvent(channelPosition, status, "");
    }

    protected void publishEvent(ChannelPosition channelPosition, EventSourceNotification.Status status, String message) {
        if (eventPublisher != null) {
            EventSourceNotification notification = EventSourceNotification.builder()
                    .withEventSourceName(name)
                    .withStreamName(getStreamName())
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
