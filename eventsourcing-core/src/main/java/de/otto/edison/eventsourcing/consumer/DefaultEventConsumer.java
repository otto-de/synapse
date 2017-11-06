package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.state.StateRepository;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class DefaultEventConsumer<T> implements EventConsumer<T> {

    private static final Logger LOG = getLogger(DefaultEventConsumer.class);

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
    public void accept(final Event<T> event) {
        stateRepository.put(event.key(), event.payload());
    }

}
