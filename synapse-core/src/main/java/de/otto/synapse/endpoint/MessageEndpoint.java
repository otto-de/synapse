package de.otto.synapse.endpoint;

import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Endpoint that is used by an application to access the messaging infrastructure to send or receive messages.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
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
     * Intercepts a message using all registered {@link MessageInterceptor interceptors} and returns the
     * resulting message.
     *
     * <p>
     *     If {@code null} is returned, the message must be dropped by the {@link MessageEndpoint}.
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
    default Message<String> intercept(final Message<String> message) {
        return message;
    }

}
