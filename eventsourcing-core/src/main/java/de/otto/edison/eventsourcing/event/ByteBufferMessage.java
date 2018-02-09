package de.otto.edison.eventsourcing.event;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static de.otto.edison.eventsourcing.event.Header.emptyHeader;
import static de.otto.edison.eventsourcing.event.Header.responseHeader;

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

    public static ByteBufferMessage message(final String key,
                                            final ByteBuffer payload) {
        return new ByteBufferMessage(
                key, emptyHeader(), payload);
    }

    public static ByteBufferMessage message(final String key,
                                            final Header header,
                                            final ByteBuffer payload) {
        return new ByteBufferMessage(
                key, header, payload);
    }

    protected ByteBufferMessage(final String key,
                                final Header header,
                                final ByteBuffer payload) {
        super(key, header, payload);
    }

}
