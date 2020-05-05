package de.otto.synapse.endpoint;

import de.otto.synapse.channel.selector.Selector;

import javax.annotation.Nonnull;

/*
 * A factory used to create {@link MessageSenderEndpoint} instances.
 *
 */
public interface MessageEndpointFactory<T extends MessageEndpoint> {

    /**
     * Creates and returns a {@link MessageEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageEndpoint}
     * @return MessagerSenderEndpoint
     */
    T create(@Nonnull String channelName);

    /**
     * Returns true if the factory is capable to create a {@link MessageEndpoint} matching the given
     * selector, false otherwise.
     *
     * @param channelSelector the selector
     * @return boolean
     */
    boolean matches(final Class<? extends Selector> channelSelector);

    Class<? extends Selector> selector();
}
