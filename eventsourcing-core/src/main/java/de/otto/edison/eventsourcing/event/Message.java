package de.otto.edison.eventsourcing.event;

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
 * @param <T>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html">EIP - Message</a>
 */
public class Message<T> {

    public static <T> Message<T> message(final String key,
                                         final T payload) {
        return new Message<>(
                key, emptyHeader(), payload);
    }

    public static <T> Message<T> message(final String key,
                                         final Header header,
                                         final T payload) {
        return new Message<>(
                key, header, payload);
    }

    @Deprecated
    public static <T> Message<T> message(final String key,
                                         final T payload,
                                         final String sequenceNumber,
                                         final Instant approximateArrivalTimestamp) {
        return new Message<>(
                key,
                responseHeader(sequenceNumber, approximateArrivalTimestamp, null),
                payload);
    }

    @Deprecated
    public static <T> Message<T> message(final String key,
                                         final T payload,
                                         final String sequenceNumber,
                                         final Instant approximateArrivalTimestamp,
                                         final Duration durationBehind) {
        return new Message<>(
                key,
                responseHeader(sequenceNumber, approximateArrivalTimestamp, durationBehind),
                payload);
    }

    // TODO: Message sollte keinen key haben. Eine abgeleitete, spezielle Message einführen oder den Key zum Header hinzufügen.
    private final String key;
    private final Header header;
    private final T payload;


    protected Message(final String key,
                      final Header header,
                      final T payload) {
        this.key = key;
        this.payload = payload;
        this.header = header;
    }

    public String getKey() {
        return key;
    }

    public T getPayload() {
        return payload;
    }

    public Header getHeader() {
        return header;
    }

    @Deprecated
    public String getSequenceNumber() {
        return header.getSequenceNumber();
    }

    @Deprecated
    public Instant getArrivalTimestamp() {
        return header.getArrivalTimestamp();
    }

    @Deprecated
    public Optional<Duration> getDurationBehind() {
        return header.getDurationBehind();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message<?> message = (Message<?>) o;
        return Objects.equals(key, message.key) &&
                Objects.equals(payload, message.payload) &&
                Objects.equals(header, message.header);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, payload, header);
    }

    @Override
    public String toString() {
        return "Message{" +
                "key='" + key + '\'' +
                ", payload=" + payload +
                ", header=" + header +
                '}';
    }
}
