package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.consumer.DispatchingMessageConsumer;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageEndpoint;

/*+
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 */
public interface MessageReceiverEndpoint extends MessageEndpoint {

    /**
     * Registers a new EventConsumer at the EventSource.
     *
     * {@link MessageConsumer consumers} have to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param messageConsumer registered EventConsumer
     * @param payloadType the type of the consumer's accepted message payload
     * @param <T> the type of the consumer's accepted message payload
     */
    <T> void register(MessageConsumer<T> messageConsumer, Class<T> payloadType);
    // TODO: <T> void register(MessageConsumer<TypeReference<T>> messageConsumer, TypeReference<T> payloadType);

    /**
     * Returns registered EventConsumers.
     *
     * @return EventConsumers
     */
    DispatchingMessageConsumer dispatchingMessageConsumer();

    /**
     * Takes zero or more messages from the channel, calls the interceptor-chain for all messages, and notifies
     * the registered consumers.
     *
     * depending on the number of messages currently available in the channel and
     * possibly some implementation-specific things like, for example, configured batch-sizes.)
     * @param channelPosition specifies where to start fetching the next messages
     * @return the new ChannelPosition used to continue to fetch more messages
     */
    // MessageLogReceiverEndpoint: ChannelPosition consume(final ChannelPosition channelPosition);
    // Channel: void consume();
}
