package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.endpoint.MessageEndpointFactory;

import javax.annotation.Nonnull;

/*
 * A factory used to create {@link MessageLogReceiverEndpoint} instances.
 *
 */
public interface MessageLogReceiverEndpointFactory extends MessageEndpointFactory<MessageLogReceiverEndpoint> {

    /**
     * Creates and returns a {@link MessageLogReceiverEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageLogReceiverEndpoint}
     * @return MessageLogReceiverEndpoint
     */
    MessageLogReceiverEndpoint create(@Nonnull String channelName);

}
