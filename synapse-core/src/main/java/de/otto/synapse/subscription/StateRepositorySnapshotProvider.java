package de.otto.synapse.subscription;

import de.otto.synapse.message.Message;
import de.otto.synapse.state.StateRepository;

import java.util.stream.Stream;

import static java.util.Collections.singletonList;

/**
 * A SnapshotProvider that is relying on a {@link StateRepository} to generate the snapshot messages
 * for a given entity-id.
 * <p>
 *     Using an {@link EntityToMessageListTransformer}, the entities stored in the StateRepository can be
 *     transformed into a sequence of messages. This is especially required, if the representation of the
 *     entity does not match the payload of the snapshot messages.
 * </p>
 * @param <E>
 */
public class StateRepositorySnapshotProvider<E> implements SnapshotProvider {
    private final String channelName;
    private final StateRepository<E> stateRepository;
    private final EntityToMessageListTransformer<E> entityToMessagesTransformer;

    public StateRepositorySnapshotProvider(final String channelName,
                                           final StateRepository<E> stateRepository) {
        this(channelName, stateRepository, (String key, E payload) -> singletonList(Message.message(key, payload)));
    }

    public StateRepositorySnapshotProvider(final String channelName,
                                           final StateRepository<E> stateRepository,
                                           final EntityToMessageListTransformer<E> entityToMessagesTransformer) {
        this.channelName = channelName;
        this.stateRepository = stateRepository;
        this.entityToMessagesTransformer = entityToMessagesTransformer;
    }

    @Override
    public String channelName() {
        return channelName;
    }

    @Override
    public Stream<? extends Message<?>> snapshot(final String entityId) {
        return stateRepository.get(entityId)
                .map((E t) -> entityToMessagesTransformer.apply(entityId, t).stream())
                .orElse(Stream.empty());
    }
}
