package de.otto.synapse.endpoint;

import de.otto.synapse.channel.selector.Selector;

/*
 * A type that is selectable.
 *
 */
public interface Selectable {

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
