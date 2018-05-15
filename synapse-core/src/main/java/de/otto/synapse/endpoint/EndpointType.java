package de.otto.synapse.endpoint;

/**
 * Enumeration to distinguish sender and receiver {@link MessageEndpoint endpoints}
 */
public enum EndpointType {
    /**
     * The {@link MessageEndpoint} IS-A {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint}
     */
    SENDER,
    /**
     * The {@link MessageEndpoint} IS-A {@link de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint}
     */
    RECEIVER
}
