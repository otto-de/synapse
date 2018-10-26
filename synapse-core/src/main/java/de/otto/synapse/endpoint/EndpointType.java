package de.otto.synapse.endpoint;

import de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;

/**
 * Enumeration to distinguish sender and receiver {@link MessageEndpoint endpoints}
 */
public enum EndpointType {
    /**
     * The {@link MessageEndpoint} IS-A {@link MessageSenderEndpoint}
     */
    SENDER,
    /**
     * The {@link MessageEndpoint} IS-A {@link MessageReceiverEndpoint}
     */
    RECEIVER
}
