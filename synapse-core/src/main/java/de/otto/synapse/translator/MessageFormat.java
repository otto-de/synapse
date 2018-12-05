package de.otto.synapse.translator;

/**
 * Identifies the format used to encode or decode messages.
 */
public enum MessageFormat {
    /** String representation of the message only contains the message payload; no header attributes supported. */
    V1,
    /** String representation of the message contains version, header attributes and payload in JSON format */
    V2;

    /**
     * Returns the default message format used by Synapse to encode messages.
     *
     * @return the default message format
     */
    public static MessageFormat defaultMessageFormat() {
        return V1;
    }
}
