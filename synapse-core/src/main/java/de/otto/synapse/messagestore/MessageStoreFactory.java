package de.otto.synapse.messagestore;

import javax.annotation.Nonnull;

/**
 * A factory used to create {@link MessageStore} instances.
 */
@FunctionalInterface
public interface MessageStoreFactory<T extends MessageStore> {

    /**
     * Creates and returns a {@link MessageStore}.
     *
     * @param name the name of the {@code MessageStore}
     * @param channelName the name of the channel associated to the {@code MessageStore}.
     * @return MessageStore
     */
    T createMessageStoreFor(@Nonnull String name, @Nonnull String channelName);

}
