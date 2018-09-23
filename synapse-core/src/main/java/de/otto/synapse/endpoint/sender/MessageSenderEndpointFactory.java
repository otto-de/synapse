package de.otto.synapse.endpoint.sender;

import javax.annotation.Nonnull;

/*
 * A factory used to create {@link MessageSenderEndpoint} instances.
 *
 */
public interface MessageSenderEndpointFactory {

    /**
     * Creates and returns a {@link AbstractMessageSenderEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageSenderEndpoint}
     * @return MessagerSenderEndpoint
     */
    MessageSenderEndpoint create(@Nonnull String channelName);

    /**
     * @return true if the factory is able to support the given channel name, false otherwise.
     */
    boolean supportsChannel(final String channelName);
}
