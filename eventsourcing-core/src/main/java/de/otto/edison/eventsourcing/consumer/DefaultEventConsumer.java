package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.state.StateRepository;

import java.util.function.Consumer;

public class DefaultEventConsumer<T> implements EventConsumer<T> {

    private final String streamName;
    private final String keyPattern;
    private final StateRepository<T> stateRepository;

    public DefaultEventConsumer(final String streamName,
                                final String keyPattern,
                                final StateRepository<T> stateRepository) {
        this.streamName = streamName;
        this.stateRepository = stateRepository;
        this.keyPattern = keyPattern;
    }

    @Override
    public String streamName() {
        return streamName;
    }

    @Override
    public Consumer<Event<T>> consumerFunction() {
        return this::accept;
    }

    @Override
    public String getKeyPattern() {
        return keyPattern;
    }


    private void accept(final Event<T> event) {
        stateRepository.put(event.key(), event.payload());
    }

}
