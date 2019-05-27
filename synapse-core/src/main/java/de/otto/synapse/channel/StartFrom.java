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
     * Start consumption of messages at the specified position.
     */
    AT_POSITION,
    /**
     * Start consumption of messages at first message at the specified timestamp.
     */
    TIMESTAMP,
    /**
     * Start reading just after the most recent record in the shard, so that you always read the most recent data in the shard.
     */
    LATEST
}
