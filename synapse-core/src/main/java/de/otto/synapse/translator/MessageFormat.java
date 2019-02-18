package de.otto.synapse.translator;

import java.util.regex.Pattern;

/**
 * Identifies the format used to encode or decode messages.
 */
public enum MessageFormat {

    /** String representation of the message only contains the message payload; no header attributes supported. */
    V1,
    /** String representation of the message contains version, header attributes and payload in JSON format */
    V2;

    public static final String SYNAPSE_MSG_FORMAT = "_synapse_msg_format";
    public static final String SYNAPSE_MSG_KEY = "_synapse_msg_key";
    public static final String SYNAPSE_MSG_COMPACTIONKEY = "compactionKey";
    public static final String SYNAPSE_MSG_PARTITIONKEY = "partitionKey";
    public static final String SYNAPSE_MSG_HEADERS = "_synapse_msg_headers";
    public static final String SYNAPSE_MSG_PAYLOAD = "_synapse_msg_payload";

    private static final Pattern V2_PATTERN = Pattern.compile("\\{\\s*\"" + SYNAPSE_MSG_FORMAT + "\"\\s*:\\s*\"v2\".+");

    /**
     * Returns the default message format used by Synapse to encode messages.
     *
     * @return the default message format
     */
    public static MessageFormat defaultMessageFormat() {
        return V1;
    }

    /**
     * Determines the {@code MessageFormat} from the string representation of some message.
     *
     * @param message the string representation of some message
     * @return MessageFormat of the message
     */
    public static MessageFormat versionOf(final String message) {
        if (message != null) {
            return V2_PATTERN.matcher(message).matches()
                    ? V2
                    : V1;
        } else {
            return V1;
        }
    }}
