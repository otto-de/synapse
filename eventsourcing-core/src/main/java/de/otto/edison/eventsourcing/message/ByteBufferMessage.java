package de.otto.edison.eventsourcing.message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static de.otto.edison.eventsourcing.message.Header.emptyHeader;
import static de.otto.edison.eventsourcing.message.Header.responseHeader;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.*;

/**
 * A Message is an atomic packet of data that can be transmitted on a channel.
 *
 * <p>
 * <img src="http://www.enterpriseintegrationpatterns.com/img/MessageSolution.gif" />
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

    public static ByteBufferMessage byteBufferMessage(final String key,
                                                      final ByteBuffer payload) {
        return new ByteBufferMessage(
                key, emptyHeader(), payload);
    }

    public static ByteBufferMessage byteBufferMessage(final String key,
                                                      final String payload) {
        return new ByteBufferMessage(
                key, emptyHeader(), wrap(payload.getBytes(UTF_8)));
    }

    public static ByteBufferMessage byteBufferMessage(final String key,
                                                      final Header header,
                                                      final ByteBuffer payload) {
        return new ByteBufferMessage(
                key, header, payload);
    }

    public static ByteBufferMessage byteBufferMessage(final String key,
                                                      final Header header,
                                                      final String payload) {
        return new ByteBufferMessage(
                key, header, wrap(payload.getBytes(UTF_8)));
    }

    protected ByteBufferMessage(final String key,
                                final Header header,
                                final ByteBuffer payload) {
        super(key, header, payload);
    }

}
