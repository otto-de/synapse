package de.otto.synapse.eventsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.StreamPosition;
import de.otto.synapse.consumer.DispatchingMessageConsumer;
import de.otto.synapse.consumer.EventSourceNotification;
import de.otto.synapse.consumer.MessageConsumer;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.slf4j.LoggerFactory.getLogger;

public abstract class AbstractEventSource implements EventSource {

    private static final Logger LOG = getLogger(AbstractEventSource.class);

    private final String name;
    private final ApplicationEventPublisher eventPublisher;
    private final DispatchingMessageConsumer dispatchingMessageConsumer;
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    public AbstractEventSource(final String name,
                               final ApplicationEventPublisher eventPublisher,
                               final ObjectMapper objectMapper) {
        this.name = name;
        this.eventPublisher = eventPublisher;
        this.dispatchingMessageConsumer = new DispatchingMessageConsumer(objectMapper);
    }

    public String getName() {
        return name;
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
        dispatchingMessageConsumer.add(messageConsumer);
    }

    /**
     * Returns the list of registered EventConsumers.
     *
     * @return list of registered EventConsumers
     */
    @Override
    public DispatchingMessageConsumer dispatchingMessageConsumer() {
        return dispatchingMessageConsumer;
    }

    protected void publishEvent(StreamPosition streamPosition, EventSourceNotification.Status status) {
        publishEvent(streamPosition, status, "");
    }

    protected void publishEvent(StreamPosition streamPosition, EventSourceNotification.Status status, String message) {
        if (eventPublisher != null) {
            EventSourceNotification notification = EventSourceNotification.builder()
                    .withEventSourceName(name)
                    .withStreamName(getStreamName())
                    .withStreamPosition(streamPosition)
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
