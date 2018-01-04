package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.state.StateRepository;

import java.util.regex.Pattern;

public class DefaultEventConsumer<T> implements EventConsumer<T> {

    private final String streamName;
    private final Pattern keyPattern;
    private final StateRepository<T> stateRepository;
    private final Class<T> payloadType;

    public DefaultEventConsumer(final String streamName,
                                final String keyPattern,
                                final Class<T> payloadType,
                                final StateRepository<T> stateRepository) {
        this.streamName = streamName;
        this.keyPattern = Pattern.compile(keyPattern);
        this.payloadType = payloadType;
        this.stateRepository = stateRepository;
    }

    @Override
    public String streamName() {
        return streamName;
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
        stateRepository.put(event.key(), event.payload());
    }

}
