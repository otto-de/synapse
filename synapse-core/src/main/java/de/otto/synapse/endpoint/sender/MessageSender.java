package de.otto.synapse.endpoint.sender;

import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Endpoint that is used by an application to send messages to a messaging channel.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 *
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageEndpoint.html">EIP: Message Endpoint</a>
 */

public interface MessageSender {

    /**
     * Send a single {@link Message} to the channel.
     *
     * @param message the message
     * @return a CompletableFuture that can be used for logging, error handling, waiting for completion, etc.
     */
    CompletableFuture<Void> send(@Nonnull Message<?> message);

    /**
     * Send a batch of {@link Message messages} to the channel.
     *
     * @param batch the batch of messages
     * @return a CompletableFuture that can be used for logging, error handling, waiting for completion, etc.
     */
    CompletableFuture<Void> sendBatch(@Nonnull Stream<? extends Message<?>> batch);

}
