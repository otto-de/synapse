package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.message.Message;

import java.util.concurrent.CompletableFuture;

/**
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel with Queue or FIFO semantics.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 * <p>
 *     {@code MessageQueueReceiverEndpoints} are Message Endpoints for Point-to-Point Channels:
 * </p>
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/PointToPointSolution.gif" alt="Point-to-Point Channel">
 * </p>
 *
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageEndpoint.html">EIP: Message Endpoint</a>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/PointToPointChannel.html">EIP: Point-to-Point Channel</a>
 */
public interface MessageQueueReceiverEndpoint extends MessageReceiverEndpoint {

    /**
     * Takes zero or more messages from the channel, calls {@link #intercept(Message)} for every message, and notifies
     * the registered consumers with the intercepted message, or drops the message, if {@code intercept} returns null.
     *
     * <p>
     *     Consumption starts with the earliest available and not-yet-consumed message and finishes when either the
     *     {@code stopCondition} is met, or the application is shutting down.
     * </p>
     * <p>
     *     The {@link #register(MessageConsumer) registered} {@link MessageConsumer consumers} are used as a
     *     callback for consumed messages. A {@link MessageDispatcher} can be used as a consumer, if multiple
     *     consumers, or consumers with {@link Message#getPayload() message payloads} other than {@code String} are
     *     required.
     * </p>
     * @return completable future that can be used to {@link CompletableFuture#join() wait} until the endpoint has
     *         stopped message consumption.
     */
    CompletableFuture<Void> consume();

    /**
     * Stops consumption of messages and shuts down the {@code MessageQueueReceiverEndpoint}.
     */
    void stop();

}
