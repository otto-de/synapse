package de.otto.synapse.message;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

/**
 * The Header of a {@link Message}.
 * <p>
 *     Headers contain metadata about a message which may only be available on one side of a
 *     channel: {@link MessageReceiverEndpoint} and
 *     {@link MessageSenderEndpoint} might add different information to
 *     the message header.
 * </p>
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageSolution.gif" alt="Message">
 * </p>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html">EIP: Message</a>
 */
public class Header implements Serializable {

    // TODO: Header extends ImmutableMultimap<String, Object>

    public static Header emptyHeader() {
        return new Header(null, now(), null);
    }

    public static Header responseHeader(final ShardPosition shardPosition,
                                        final Instant arrivalTimestamp,
                                        final Duration durationBehind) {
        return new Header(
                shardPosition,
                arrivalTimestamp,
                durationBehind);
    }

    public static Header responseHeader(final ShardPosition shardPosition,
                                        final Instant arrivalTimestamp) {
        return new Header(
                shardPosition,
                arrivalTimestamp,
                null);
    }

    private final ShardPosition shardPosition;
    private final Instant arrivalTimestamp;
    private final Duration durationBehind;

    private Header(final ShardPosition shardPosition,
                   final Instant approximateArrivalTimestamp,
                   final Duration durationBehind) {
        this.shardPosition = shardPosition;
        this.arrivalTimestamp = requireNonNull(approximateArrivalTimestamp);
        this.durationBehind = durationBehind;
    }

    @Nonnull
    public Optional<ShardPosition> getShardPosition() {
        return Optional.ofNullable(shardPosition);
    }

    @Nonnull
    public Instant getArrivalTimestamp() {
        return arrivalTimestamp;
    }

    /**
     * Returns the approx. duration of this event behind the latest event in the event source.
     *
     * @return Duration
     */
    @Nonnull
    public Optional<Duration> getDurationBehind() {
        return Optional.ofNullable(durationBehind);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Header header = (Header) o;
        return Objects.equals(shardPosition, header.shardPosition) &&
                Objects.equals(arrivalTimestamp, header.arrivalTimestamp) &&
                Objects.equals(durationBehind, header.durationBehind);
    }

    @Override
    public int hashCode() {

        return Objects.hash(shardPosition, arrivalTimestamp, durationBehind);
    }

    @Override
    public String toString() {
        return "Header{" +
                "shardPosition=" + shardPosition +
                ", arrivalTimestamp=" + arrivalTimestamp +
                ", durationBehind=" + durationBehind +
                '}';
    }
}
