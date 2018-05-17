package de.otto.synapse.channel;

public enum StartFrom {
    /**
     * Start consumption of messages at the horizon of the message channel.
     */
    HORIZON,
    /**
     * Start consumption of messages at first message after the specified position.
     */
    POSITION,
    /**
     * Start consumption of messages at first message at the specified timestamp.
     */
    TIMESTAMP
}
