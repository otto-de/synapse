package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.StartFrom;

import javax.annotation.Nonnull;

/*
 * A factory used to create {@link MessageLogReceiverEndpoint} instances.
 *
 */
public interface MessageLogReceiverEndpointFactory {

    /**
     * Creates and returns a {@link MessageLogReceiverEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageLogReceiverEndpoint}
     * @return MessageLogReceiverEndpoint
     */
    MessageLogReceiverEndpoint create(@Nonnull String channelName);

    /**
     * Creates and returns a {@link MessageLogReceiverEndpoint} for a messaging channel.
     *
     * @param channelName the name of the channel of the created {@code MessageLogReceiverEndpoint}
     * @param iteratorAt position for ShardIterator
     * @return MessageLogReceiverEndpoint
     */
    MessageLogReceiverEndpoint create(@Nonnull String channelName, @Nonnull StartFrom iteratorAt);

}
