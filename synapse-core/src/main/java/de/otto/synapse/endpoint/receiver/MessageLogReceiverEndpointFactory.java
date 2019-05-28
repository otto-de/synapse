package de.otto.synapse.endpoint.receiver;

import javax.annotation.Nonnull;

/*
 * A factory used to create {@link MessageLogReceiverEndpoint} instances.
 *
 */
@FunctionalInterface
public interface MessageLogReceiverEndpointFactory {

    /**
     * Creates and returns a {@link MessageLogReceiverEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageLogReceiverEndpoint}
     * @return MessageLogReceiverEndpoint
     */
    MessageLogReceiverEndpoint create(@Nonnull String channelName);

}
