package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.state.StateRepository;

public class DefaultEventConsumer<T> implements EventConsumer<T> {

    private final String streamName;
    private final StateRepository<T> stateRepository;

    public DefaultEventConsumer(final String streamName,
                                final StateRepository<T> stateRepository) {
        this.streamName = streamName;
        this.stateRepository = stateRepository;
    }

    @Override
    public String streamName() {
        return streamName;
    }

    @Override
    public void accept(final Event<T> event) {
        stateRepository.put(event.key(), event.payload());
    }

}
