package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.state.StateRepository;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class DefaultEventConsumer<T> implements EventConsumer<T> {

    private static final Logger LOG = getLogger(DefaultEventConsumer.class);

    private final StateRepository<T> stateRepository;
    private int eventCount = 0;

    public DefaultEventConsumer(final StateRepository<T> stateRepository) {
        this.stateRepository = stateRepository;
    }

    @Override
    public void init(final String eventSource) {
        LOG.info("Started consuming of events from eventSource " + eventSource);
    }

    @Override
    public void completed(final String eventSource) {
        LOG.info("Finished consuming {} events from snapshot {}", eventCount, eventSource);
    }

    @Override
    public void aborted(final String eventSource) {
        LOG.info("Aborted consuming of events from {} after error", eventSource);
    }

    @Override
    public void accept(final Event<T> event) {
        stateRepository.put(event.key(), event.payload());
        ++eventCount;
    }

}
