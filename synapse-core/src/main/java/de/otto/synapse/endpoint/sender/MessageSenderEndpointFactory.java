package de.otto.synapse.endpoint.sender;

import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.MessageEndpointFactory;
import de.otto.synapse.translator.MessageFormat;
import jakarta.annotation.Nonnull;

import static de.otto.synapse.translator.MessageFormat.defaultMessageFormat;

/*
 * A factory used to create {@link MessageSenderEndpoint} instances.
 *
 */
public interface MessageSenderEndpointFactory extends MessageEndpointFactory<MessageSenderEndpoint> {

    /**
     * Creates and returns a {@link AbstractMessageSenderEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageSenderEndpoint}
     * @return MessagerSenderEndpoint
     */
    default MessageSenderEndpoint create(@Nonnull String channelName) {
        return create(channelName, defaultMessageFormat());
    }

    /**
     * Creates and returns a {@link AbstractMessageSenderEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageSenderEndpoint}
     * @param messageFormat the message format in which messages are serialized to
     * @return MessagerSenderEndpoint
     */
    MessageSenderEndpoint create(@Nonnull String channelName, MessageFormat messageFormat);

    /**
     * Returns true if the factory is capable to create a {@link MessageSenderEndpoint} matching the given
     * selector, false otherwise.
     *
     * @param channelSelector channel selector
     * @return boolean
     */
    boolean matches(final Class<? extends Selector> channelSelector);

    Class<? extends Selector> selector();
}
