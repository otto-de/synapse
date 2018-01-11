package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractEventSource implements EventSource {

    private final String name;
    private final EventConsumers eventConsumers;

    public AbstractEventSource(final String name, final ObjectMapper objectMapper) {
        this.name = name;
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

}
