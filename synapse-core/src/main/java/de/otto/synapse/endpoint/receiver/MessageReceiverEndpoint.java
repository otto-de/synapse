package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.MessageEndpoint;
import jakarta.annotation.Nonnull;

/**
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 */
public interface MessageReceiverEndpoint extends MessageEndpoint {

    /**
     * Registers a MessageConsumer at the receiver endpoint.
     *
     * {@link MessageConsumer consumers} have to be thread safe as they might be called from multiple threads
     * in parallel (e.g. for kinesis streams there is one thread per shard).
     *
     * @param messageConsumer registered EventConsumer
     */
    void register(MessageConsumer<?> messageConsumer);

    /**
     * Returns the MessageDispatcher that is used to dispatch messages.
     *
     * @return MessageDispatcher
     */
    @Nonnull
    MessageDispatcher getMessageDispatcher();
}
