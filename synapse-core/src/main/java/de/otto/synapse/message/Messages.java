package de.otto.synapse.message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

import static de.otto.synapse.message.Header.emptyHeader;
import static de.otto.synapse.message.Message.message;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A Message is an atomic packet of data that can be transmitted on a channel.
 *
 * <p>
 * <img src="http://www.enterpriseintegrationpatterns.com/img/MessageSolution.gif" alt="Message">
 * </p>
 *
 * <p>Thus to transmit data, an application must break the data into one or more packets,
 * wrap each packet as a message, and then send the message on a channel. Likewise, a receiver
 * application receives a message and must extract the data from the message to process it.
 * </p>
 * <p>
 * The message system will try repeatedly to deliver the message (e.g., transmit it from the
 * sender to the receiver) until it succeeds.
 * </p>
 *
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html">EIP: Message</a>
 */
public final class Messages {

    public static Message<ByteBuffer> byteBufferMessage(final @Nonnull Key key,
                                                        final @Nullable ByteBuffer payload) {
        return message(key, emptyHeader(), payload);
    }

    public static Message<ByteBuffer> byteBufferMessage(final @Nonnull Key key,
                                                        final @Nonnull Header header,
                                                        final @Nullable ByteBuffer payload) {
        return message(key, header, payload);
    }

    public static Message<ByteBuffer> byteBufferMessage(final @Nonnull Key key,
                                                        final @Nullable String payload) {
        final ByteBuffer byteBuffer = payload != null ? wrap(payload.getBytes(UTF_8)) : null;
        return message(key, emptyHeader(), byteBuffer);
    }

    public static Message<ByteBuffer> byteBufferMessage(final @Nonnull Key key,
                                                        final @Nonnull Header header,
                                                        final @Nullable String payload) {
        final ByteBuffer byteBuffer = payload != null ? wrap(payload.getBytes(UTF_8)) : null;
        return message(key, header, byteBuffer);
    }

    public static Message<String> stringMessage(@Nonnull final Key key,
                                                @Nullable final String payload) {
        return message(key, emptyHeader(), payload);
    }

    public static Message<String> stringMessage(@Nonnull final Key key,
                                                @Nonnull final Header header,
                                                @Nullable final String payload) {
        return message(key, header, payload);
    }
}
