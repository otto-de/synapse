package de.otto.synapse.message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;

import static de.otto.synapse.message.Header.of;

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
 * @param <P> The type of the Message payload
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html">EIP: Message</a>
 */
public class Message<P> implements Serializable {

    public static <P> Message<P> message(final @Nonnull Key key,
                                         final @Nullable P payload) {
        return new Message<>(key, of(), payload);
    }

    public static <P> Message<P> message(final @Nonnull Key key,
                                         final @Nonnull Header header,
                                         final @Nullable P payload) {
        return new Message<>(key, header, payload);
    }

    public static <P> Message<P> message(final @Nonnull String key,
                                         final @Nullable P payload) {
        return new Message<>(Key.of(key), of(), payload);
    }

    public static <P> Message<P> message(final @Nonnull String key,
                                         final @Nonnull Header header,
                                         final @Nullable P payload) {
        return new Message<>(Key.of(key), header, payload);
    }

    private final Key key;
    private final Header header;
    private final P payload;

    protected Message(final @Nonnull Key key,
                      final @Nonnull Header header,
                      final @Nullable P payload) {
        this.key = key;
        this.payload = payload;
        this.header = header;
    }

    @Nonnull
    public Key getKey() {
        return key;
    }

    @Nullable
    public P getPayload() {
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
                "of='" + key + '\'' +
                ", payload=" + payload +
                ", header=" + header +
                '}';
    }

    public static <P> Builder<P> builder(final Class<P> payloadType) {
        return new Builder<>();
    }

    public static <P> Builder<P> copyOf(final Message<P> message) {
        return new Builder<P>()
                .withKey(message.getKey())
                .withHeader(message.getHeader())
                .withPayload(message.getPayload());
    }

    public static class Builder<P> {
        private Key key = Key.of();
        private Header header;
        private P payload;

        public Builder<P> withKey(final String key) {
            this.key = Key.of(key);
            return this;
        }

        public Builder<P> withKey(final Key key) {
            this.key = key;
            return this;
        }

        public Builder<P> withHeader(final Header header) {
            this.header = header;
            return this;
        }

        public Builder<P> withPayload(final P payload) {
            this.payload = payload;
            return this;
        }

        public Message<P> build() {
            return new Message<>(key, header, payload);
        }
    }
}
