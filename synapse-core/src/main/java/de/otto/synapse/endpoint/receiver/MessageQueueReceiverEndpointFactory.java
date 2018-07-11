package de.otto.synapse.endpoint.receiver;

import javax.annotation.Nonnull;

/*
 * A factory used to create {@link MessageQueueReceiverEndpoint} instances.
 *
 */
@FunctionalInterface
public interface MessageQueueReceiverEndpointFactory {

    /**
     * Creates and returns a {@link MessageQueueReceiverEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageQueueReceiverEndpoint}
     * @return MessageQueueReceiverEndpoint
     */
    MessageQueueReceiverEndpoint create(@Nonnull String channelName);

}
