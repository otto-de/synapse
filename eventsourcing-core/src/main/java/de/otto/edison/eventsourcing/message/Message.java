package de.otto.edison.eventsourcing.message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

import static de.otto.edison.eventsourcing.message.Header.emptyHeader;

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
 * @param <T> The type of the Message payload
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html">EIP - Message</a>
 */
public class Message<T> {

    public static <T> Message<T> message(final @Nonnull String key,
                                         final @Nullable T payload) {
        return new Message<>(key, emptyHeader(), payload);
    }

    public static <T> Message<T> message(final @Nonnull String key,
                                         final @Nonnull Header header,
                                         final @Nullable T payload) {
        return new Message<>(key, header, payload);
    }

    // TODO: Message sollte keinen key haben. Eine abgeleitete, spezielle Message einführen oder den Key zum Header hinzufügen.
    // TODO: Von einer DocumentMessage könnte man eventuell einen Entity-Key oder einen aus Entity-Key +
    // TODO: Message-Type zusammengesetzten Schlüssel erwarten.
    // TODO: Kinesis verwendet beispielsweise nur einen "Partition-Key", der (anders als bei Kafka)
    // TODO: nicht für die Compaction, sondern nur für die Partitionierung verwendet wird. Insofern:
    // TODO: Key als Aggregate aus partitionKey(), entityId(), messageType()
    private final String key;
    private final Header header;
    private final T payload;

    protected Message(final @Nonnull String key,
                      final @Nonnull Header header,
                      final @Nullable T payload) {
        this.key = key;
        this.payload = payload;
        this.header = header;
    }

    @Nonnull
    public String getKey() {
        return key;
    }

    @Nullable
    public T getPayload() {
        return payload;
    }

    @Nonnull
    public Header getHeader() {
        return header;
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
