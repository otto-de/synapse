package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.message.Message;
import de.otto.edison.eventsourcing.state.StateRepository;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

public class DefaultMessageConsumer<T> implements MessageConsumer<T> {

    private final Pattern keyPattern;
    private final StateRepository<T> stateRepository;
    private final Class<T> payloadType;

    public DefaultMessageConsumer(final String keyPattern,
                                  final Class<T> payloadType,
                                  final StateRepository<T> stateRepository) {
        this.keyPattern = Pattern.compile(keyPattern);
        this.payloadType = payloadType;
        this.stateRepository = stateRepository;
    }

    /**
     * Returns the expected payload type of {@link Message events} consumed by this EventConsumer.
     *
     * @return payload type
     */
    @Nonnull
    @Override
    public Class<T> payloadType() {
        return payloadType;
    }

    @Nonnull
    @Override
    public Pattern keyPattern() {
        return keyPattern;
    }

    @Override
    public void accept(final Message<T> message) {
        if (message.getPayload() == null) {
            stateRepository.remove(message.getKey());
        } else {
            stateRepository.put(message.getKey(), message.getPayload());
        }
    }

}
