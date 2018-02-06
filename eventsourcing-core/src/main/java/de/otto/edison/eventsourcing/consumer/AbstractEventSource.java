package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import static org.slf4j.LoggerFactory.getLogger;

public abstract class AbstractEventSource implements EventSource {
    private static final Logger LOG = getLogger(AbstractEventSource.class);


    private final String name;
    private ApplicationEventPublisher eventPublisher;
    private final EventConsumers eventConsumers;

    public AbstractEventSource(final String name, final ApplicationEventPublisher eventPublisher, final ObjectMapper objectMapper) {
        this.name = name;
        this.eventPublisher = eventPublisher;
        this.eventConsumers = new EventConsumers(objectMapper);
    }

    public String getName() {
        return name;
    }

    /**
     * Registers a new EventConsumer at the EventSource.
     * <p>
     * {@link EventConsumer consumers} have to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param eventConsumer the EventConsumer that is registered at the EventSource
     */
    @Override
    public void register(final EventConsumer<?> eventConsumer) {
        eventConsumers.add(eventConsumer);
    }

    /**
     * Returns the list of registered EventConsumers.
     *
     * @return list of registered EventConsumers
     */
    @Override
    public EventConsumers registeredConsumers() {
        return eventConsumers;
    }

    protected void publishEvent(StreamPosition streamPosition, EventSourceNotification.Status status) {
        if (eventPublisher != null) {
            EventSourceNotification notification = EventSourceNotification.builder()
                    .withEventSource(this)
                    .withStreamPosition(streamPosition)
                    .withStatus(status)
                    .build();
            try {
                eventPublisher.publishEvent(notification);
            } catch (Exception e) {
                LOG.error("error publishing event source notification: {}", notification, e);
            }
        }
    }
}
