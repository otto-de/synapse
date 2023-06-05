package de.otto.synapse.messagestore;

import jakarta.annotation.Nonnull;

/**
 * A factory used to create {@link MessageStore} instances.
 */
@FunctionalInterface
public interface MessageStoreFactory<T extends MessageStore> {

    /**
     * Creates and returns a {@link MessageStore}.
     *
     * @param channelName the name of the channel associated to the {@code MessageStore}.
     * @return MessageStore
     */
    T createMessageStoreFor(@Nonnull String channelName);

}
