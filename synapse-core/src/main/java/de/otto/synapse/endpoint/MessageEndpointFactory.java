package de.otto.synapse.endpoint;

import javax.annotation.Nonnull;

/*
 * A factory used to create {@link MessageSenderEndpoint} instances.
 *
 */
public interface MessageEndpointFactory<T extends MessageEndpoint> extends Selectable {

    /**
     * Creates and returns a {@link MessageEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageEndpoint}
     * @return MessagerSenderEndpoint
     */
    T create(@Nonnull String channelName);
}
