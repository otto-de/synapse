package de.otto.synapse.endpoint.sender;

import de.otto.synapse.channel.Selector;

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
     * Returns true if the factory is capable to create a {@link MessageSenderEndpoint} matching the given
     * selectors, false otherwise.
     *
     * @param channelSelector Set of channel selectors
     * @return boolean
     */
    boolean matches(final Class<? extends Selector> channelSelector);

}
