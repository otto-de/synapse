package de.otto.synapse.endpoint;

import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.TextMessage;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Endpoint that is used by an application to access the messaging infrastructure to send or receive messages.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 * <p>
 *     The {@code MessageEndpoint} class is intended to derive {@link MessageSenderEndpoint message sender},
 *      {@link MessageQueueReceiverEndpoint message receiver} or
 *      {@link MessageLogReceiverEndpoint message-log receiver} endpoints.
 * </p>
 * <p>
 *     Message Endpoint code is custom to both the application and the messaging system’s client API.
 *     The rest of the application knows little about message formats, messaging channels, or any of
 *     the other details of communicating with other applications via messaging. It just knows that
 *     it has a request or piece of data to send to another application, or is expecting those from
 *     another application.
 * </p>
 * <p>
 *     It is the messaging endpoint code that takes that command or data, makes it into a message,
 *     and sends it on a particular messaging channel. It is the endpoint that receives a message,
 *     extracts the contents, and gives them to the application in a meaningful way.
 * </p>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageEndpoint.html">EIP: Message Endpoint</a>
 */
public interface MessageEndpoint {

    /**
     * Returns the name of the channel.
     *
     * <p>
     *     The channel name corresponds to the name of the underlying stream, queue or message log.
     * </p>
     * @return name of the channel
     */
    @Nonnull
    String getChannelName();

    /**
     * Returns the {@link InterceptorChain} of the {@code MessageEndpoint}.
     *
     * @return InterceptorChain
     */
    @Nonnull
    InterceptorChain getInterceptorChain();

    /**
     * Returns the type of the {@code MessageEndpoint}, either {@link EndpointType#SENDER} or
     * {@link EndpointType#RECEIVER}.
     *
     * @return EndpointType
     */
    @Nonnull
    EndpointType getEndpointType();

    /**
     * Intercepts a message using all registered interceptors and returns the resulting message.
     * <p>
     *     The interceptors are called in order. The result of one interceptor is propagated to the
     *     next interceptor in the chain, until the end of the chain is reached, or one interceptor
     *     has returned null.
     * </p>
     * <p>
     *     If {@code null} is returned, the message must be dropped by the {@link AbstractMessageEndpoint}.
     * </p>
     * <p>
     *     Every interceptor may transform the message, or may take additional actions like, for example,
     *     logging, monitoring or other things.
     * </p>
     *
     * @param message the message to intercept
     * @return the (possibly modified) message, or null if the message should be dropped.
     */
    @Nullable
    TextMessage intercept(@Nonnull TextMessage message);
}
