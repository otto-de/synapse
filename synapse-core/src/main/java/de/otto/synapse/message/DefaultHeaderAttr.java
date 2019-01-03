package de.otto.synapse.message;

import de.otto.synapse.configuration.SynapseProperties;

public enum DefaultHeaderAttr implements HeaderAttr {

    /**
     * Unique identifier of the message.
     *
     * <p>
     * The attribute key is {@code synapse_msg_id}
     * </p>
     */
    MSG_ID("synapse_msg_id"),
    /**
     * The sender of the message.
     * <p>
     *     Using the {@link de.otto.synapse.endpoint.DefaultSenderHeadersInterceptor}, the attribute's value
     *     is configured using {@link SynapseProperties.Sender#getName()} and defaults to the value of
     *     property {@code spring.application.name}.
     * </p>
     * <p>
     * The attribute key is {@code synapse_msg_sender}
     * </p>
     */
    MSG_SENDER("synapse_msg_sender"),
    /**
     * The header attribute containing the Kinesis {@code Record#approximateArrivalTimestamp()}
     * to which the message has arrived in Kinesis. Other channel implementations may use this attribute for similar
     * timestamps, as long as the semantics of the property is the met.
     *
     * <p>
     * The attribute key is {@code synapse_msg_arrival_ts}
     * </p>
     */
    MSG_ARRIVAL_TS("synapse_msg_arrival_ts"),
    /**
     * The timestamp to which the message was sent.
     * <p>
     * The attribute key is {@code synapse_msg_sender_ts}
     * </p>
     */
    MSG_SENDER_TS("synapse_msg_sender_ts"),
    /**
     * The timestamp to which the message was received by a {@link de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint}.
     * <p>
     * The attribute key is {@code synapse_msg_receiver_ts}
     * </p>
     */
    MSG_RECEIVER_TS("synapse_msg_receiver_ts");

    private final String key;

    DefaultHeaderAttr(final String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
