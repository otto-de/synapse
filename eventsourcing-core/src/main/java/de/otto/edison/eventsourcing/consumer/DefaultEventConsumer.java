package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.state.StateRepository;

import java.util.function.Consumer;

public class DefaultEventConsumer<T> implements EventConsumer<T> {

    private final String streamName;
    private final StateRepository<T> stateRepository;

    public DefaultEventConsumer(final String streamName,
                                final StateRepository<T> stateRepository) {
        this.streamName = streamName;
        this.stateRepository = stateRepository;
    }

    /**
     * Returns the name of the EventSource.
     * <p>
     * For streaming event-sources, this is the name of the event stream.
     * </p>
     *
     * @return name
     */
    @Override
    public String streamName() {
        return streamName;
    }

    @Override
    public Consumer<Event<T>> consumerFunction() {
        return this::accept;
    }


    private void accept(final Event<T> event) {
        stateRepository.put(event.key(), event.payload());
    }

}
