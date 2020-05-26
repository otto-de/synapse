package de.otto.synapse.subscription;

import de.otto.synapse.message.Message;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Transforms an entity from e.g. a StateRepository into a list of messages.
 * <p>
 *     The transformer is a BiFunction that will get the entity-id and the entity itself as an argument. The
 *     entity-id will most likely be used to generate one or more message-keys, while the entity will be used
 *     to generate the message payload.
 * </p>
 * @param <E>
 */
@FunctionalInterface
public interface EntityToMessageListTransformer<E> extends BiFunction<String, E, List<? extends Message<?>>> {
}
