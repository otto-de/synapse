package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.event.Event;
import de.otto.edison.eventsourcing.state.StateRepository;

import java.util.regex.Pattern;

public class DefaultEventConsumer<T> implements EventConsumer<T> {

    private final Pattern keyPattern;
    private final StateRepository<T> stateRepository;
    private final Class<T> payloadType;

    public DefaultEventConsumer(final String keyPattern,
                                final Class<T> payloadType,
                                final StateRepository<T> stateRepository) {
        this.keyPattern = Pattern.compile(keyPattern);
        this.payloadType = payloadType;
        this.stateRepository = stateRepository;
    }

    /**
     * Returns the expected payload type of {@link Event events} consumed by this EventConsumer.
     *
     * @return payload type
     */
    @Override
    public Class<T> payloadType() {
        return payloadType;
    }

    @Override
    public Pattern keyPattern() {
        return keyPattern;
    }

    @Override
    public void accept(final Event<T> event) {
        if (event.getEventBody().getPayload() == null) {
            stateRepository.remove(event.getEventBody().getKey());
        } else {
            stateRepository.put(event.getEventBody().getKey(), event.getEventBody().getPayload());
        }
    }

}
