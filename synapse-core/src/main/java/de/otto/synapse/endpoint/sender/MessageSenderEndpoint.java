package de.otto.synapse.endpoint.sender;

import de.otto.synapse.endpoint.MessageEndpoint;

/**
 * Endpoint that is used by an application to send messages to a messaging channel.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 *
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageEndpoint.html">EIP: Message Endpoint</a>
 */

public interface MessageSenderEndpoint extends MessageSender, MessageEndpoint {

}
