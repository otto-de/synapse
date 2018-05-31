package de.otto.synapse.endpoint.sender;

import javax.annotation.Nonnull;

/*
 * A factory used to create {@link MessageSenderEndpoint} instances.
 *
 */
@FunctionalInterface
public interface MessageSenderEndpointFactory {

    /**
     * Creates and returns a {@link MessageSenderEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageSenderEndpoint}
     * @return MessagerSenderEndpoint
     */
    MessageSenderEndpoint create(@Nonnull String channelName);

}
