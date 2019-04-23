package de.otto.synapse.compaction.s3;

import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.TextEncoder;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * A {@code MessageConsumer} that is used to update Snapshots.
 *
 */
public class SnapshotMessageConsumer implements MessageConsumer<String> {

    private final Pattern keyPattern = Pattern.compile(".*");
    private final StateRepository<String> stateRepository;
    private final Function<? super Message<String>, String> keyMapper = (message) -> message.getKey().compactionKey();
    private final BiFunction<Optional<String>, ? super Message<String>, String> payloadToStateMapper;

    /**
     * Creates a StatefulMessageConsumer.
     *
     * <p>
     *     The message's {@link Key#partitionKey()} is used as the key for repository entries.
     * </p>
     *
     * @param messageFormat the format used to write messages into the snapshot
     * @param stateRepository the StateRepository that is holding the State
     */
    public SnapshotMessageConsumer(final MessageFormat messageFormat,
                                   final StateRepository<String> stateRepository) {

        final TextEncoder encoder = new TextEncoder(messageFormat);
        this.stateRepository = stateRepository;
        this.payloadToStateMapper = (_previousValue, message) -> encoder.apply(message);


    }


    /**
     * Returns the expected payload type of {@link Message events} consumed by this EventConsumer.
     *
     * @return payload type
     */
    @Nonnull
    @Override
    public Class<String> payloadType() {
        return String.class;
    }

    /**
     * Returns the pattern of {@link Message#getKey() message keys} accepted by this consumer.
     *
     * @return Pattern
     */
    @Nonnull
    @Override
    public Pattern keyPattern() {
        return keyPattern;
    }

    @Override
    public void accept(final Message<String> message) {
        if (message.getPayload() == null) {
            stateRepository.remove(keyMapper.apply(message));
        } else {
            stateRepository.compute(
                    keyMapper.apply(message),
                    (_key, previousValue) -> payloadToStateMapper.apply(previousValue, message));
        }
    }

}
