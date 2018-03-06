package de.otto.synapse.endpoint;

import de.otto.synapse.message.Message;

/**
 * Message interceptors are used to intercept messages before they are sent or received by
 * {@link MessageEndpoint message endpoints}.
 *
 * <p>
 *     MessageInterceptors will usually be chained using a {@link InterceptorChain}.
 * </p>
 * <p>
 *     A {@code MessageInterceptor} can be used in different ways like, for example:
 * </p>
 * <ul>
 *     <li>Logging</li>
 *     <li>Calculating Metrics</li>
 *     <li>Wire Taps</li>
 *     <li>Message Filters</li>
 * </ul>
 * <p>
 *     ...and many other.
 * </p>
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageFilter.gif" alt="Message Filter">
 * </p>
 *
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/Filter.html">EIP: Message Filter</a>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/WireTap.html">EIP: Wire Taps</a>
 */
@FunctionalInterface
public interface MessageInterceptor {

    /**
     * Intercept a message and return the same message, a modified version of the incoming message, or null, if
     * the message should be filtered out and dropped by the {@link MessageEndpoint}
     *
     * @param message the channel-layer message with payload-type beeing a String
     * @return intercepted version of the message, or null if the message should be dropped.
     */
    Message<String> intercept(final Message<String> message);

}
