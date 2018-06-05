package de.otto.synapse.endpoint;

import de.otto.synapse.endpoint.receiver.AbstractMessageReceiverEndpoint;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;

/**
 * Enumeration to distinguish sender and receiver {@link AbstractMessageEndpoint endpoints}
 */
public enum EndpointType {
    /**
     * The {@link AbstractMessageEndpoint} IS-A {@link AbstractMessageSenderEndpoint}
     */
    SENDER,
    /**
     * The {@link AbstractMessageEndpoint} IS-A {@link AbstractMessageReceiverEndpoint}
     */
    RECEIVER
}
