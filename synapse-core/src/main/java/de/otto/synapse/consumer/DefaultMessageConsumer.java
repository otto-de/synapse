package de.otto.synapse.consumer;

import de.otto.synapse.message.Message;
import de.otto.synapse.state.concurrent.ConcurrentMapStateRepository;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

public class DefaultMessageConsumer<T> implements MessageConsumer<T> {

    private final Pattern keyPattern;
    private final ConcurrentMapStateRepository<T> stateRepository;
    private final Class<T> payloadType;

    public DefaultMessageConsumer(final String keyPattern,
                                  final Class<T> payloadType,
                                  final ConcurrentMapStateRepository<T> stateRepository) {
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
