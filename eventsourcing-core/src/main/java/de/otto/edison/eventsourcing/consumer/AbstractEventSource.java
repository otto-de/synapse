package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractEventSource implements EventSource {

    private final EventConsumers eventConsumers;

    public AbstractEventSource(final ObjectMapper objectMapper) {
        this.eventConsumers = new EventConsumers(objectMapper);
    }

    /**
     * Registers a new EventConsumer at the EventSource.
     * <p>
     * {@link EventConsumer consumers} have to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param eventConsumer
     */
    @Override
    public void register(final EventConsumer<?> eventConsumer) {
        if (eventConsumer.streamName().equals(getStreamName())) {
            eventConsumers.add(eventConsumer);
        } else {
            throw new IllegalArgumentException("Consumer does not consume events from event-stream " + getStreamName());
        }
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

}
