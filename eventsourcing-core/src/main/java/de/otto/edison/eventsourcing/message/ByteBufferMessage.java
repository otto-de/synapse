package de.otto.edison.eventsourcing.message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

import static de.otto.edison.eventsourcing.message.Header.emptyHeader;
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
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html">EIP - Message</a>
 */
public class ByteBufferMessage extends Message<ByteBuffer> {

    public static ByteBufferMessage byteBufferMessage(final @Nonnull String key,
                                                      final @Nullable ByteBuffer payload) {
        return new ByteBufferMessage(
                key, emptyHeader(), payload);
    }

    public static ByteBufferMessage byteBufferMessage(final @Nonnull String key,
                                                      final @Nonnull Header header,
                                                      final @Nullable ByteBuffer payload) {
        return new ByteBufferMessage(
                key, header, payload);
    }

    public static ByteBufferMessage byteBufferMessage(final @Nonnull String key,
                                                      final @Nullable String payload) {
        final ByteBuffer byteBuffer = payload != null ? wrap(payload.getBytes(UTF_8)) : null;
        return new ByteBufferMessage(
                key, emptyHeader(), byteBuffer);
    }

    public static ByteBufferMessage byteBufferMessage(final @Nonnull String key,
                                                      final @Nonnull Header header,
                                                      final @Nullable String payload) {
        final ByteBuffer byteBuffer = payload != null ? wrap(payload.getBytes(UTF_8)) : null;
        return new ByteBufferMessage(
                key, header, byteBuffer);
    }

    private ByteBufferMessage(final @Nonnull String key,
                              final @Nonnull Header header,
                              final @Nullable ByteBuffer payload) {
        super(key, header, payload);
    }

}
