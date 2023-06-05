package de.otto.synapse.message;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * A {@code Message} that is used by Synapse for messages with String payloads.
 *
 */
public class TextMessage extends Message<String> {


    private static final long serialVersionUID = 4270568972655971096L;

    protected TextMessage(final @Nonnull Key key,
                          final @Nonnull Header header,
                          final @Nullable String payload) {
        super(key, header, payload);
    }

    /**
     * Factory method used to create a {@code TextMessage} from another {@link Message Message&lt;String&gt;}
     *
     * @param message a message with payload-type {@code String}
     * @return TextMessage
     */
    @Nonnull
    public static TextMessage of(final @Nonnull Message<String> message) {
        return new TextMessage(message.getKey(), message.getHeader(), message.getPayload());
    }

    /**
     * Factory method used to create a {@code TextMessage} from key, header and payload parameters.
     *
     * @param key Key of the created message
     * @param header Header of the created message
     * @param payload Payload of the created message
     * @return TextMessage
     */
    @Nonnull
    public static TextMessage of(final @Nonnull Key key,
                                 final @Nonnull Header header,
                                 final @Nullable String payload) {
        return new TextMessage(key, header, payload);
    }

    /**
     * Factory method used to create a {@code TextMessage} from key, header and payload parameters.
     *
     * @param key Key of the created message
     * @param header Header of the created message
     * @param payload Payload of the created message
     * @return TextMessage
     */
    @Nonnull
    public static TextMessage of(final @Nonnull String key,
                                 final @Nonnull Header header,
                                 final @Nullable String payload) {
        return new TextMessage(Key.of(key), header, payload);
    }

    /**
     * Factory method used to create a {@code TextMessage} from key and payload parameters.
     *
     * @param key Key of the created message
     * @param payload Payload of the created message
     * @return TextMessage
     */
    @Nonnull
    public static TextMessage of(final @Nonnull Key key,
                                 final @Nullable String payload) {
        return new TextMessage(key, Header.of(), payload);
    }

    /**
     * Factory method used to create a {@code TextMessage} from key and payload parameters.
     *
     * @param key Key of the created message
     * @param payload Payload of the created message
     * @return TextMessage
     */
    public static TextMessage of(final @Nonnull String key,
                                 final @Nullable String payload) {
        return new TextMessage(Key.of(key), Header.of(), payload);
    }

}
