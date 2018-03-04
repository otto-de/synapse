package de.otto.synapse.message;

import de.otto.synapse.channel.ChannelPosition;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.Iterables.getFirst;
import static java.util.Objects.requireNonNull;

/**
 * The Header of a {@link Message}.
 *
 * <p>
 * <img src="http://www.enterpriseintegrationpatterns.com/img/MessageSolution.gif" alt="Message">
 * </p>
 *
 */
public class Header implements Serializable {
    // TODO: Header extends ImmutableMultimap<String, Object>

    public static Header emptyHeader() {
        return new Header(null, Instant.now(), null);
    }

    public static Header responseHeader(final ChannelPosition channelPosition,
                                        final Instant arrivalTimestamp,
                                        final Duration durationBehind) {
        return new Header(
                channelPosition,
                arrivalTimestamp,
                durationBehind);
    }

    public static Header responseHeader(final ChannelPosition channelPosition,
                                        final Instant arrivalTimestamp) {
        return new Header(
                channelPosition,
                arrivalTimestamp,
                null);
    }

    private final ChannelPosition channelPosition;
    private final Instant arrivalTimestamp;
    private final Duration durationBehind;

    private Header(final ChannelPosition channelPosition,
                   final Instant approximateArrivalTimestamp,
                   final Duration durationBehind) {
        if (channelPosition != null && channelPosition.shards().size() > 1) {
            throw new IllegalArgumentException("ChannelPosition must not have more than one shard");
        }
        this.channelPosition = channelPosition;
        this.arrivalTimestamp = requireNonNull(approximateArrivalTimestamp);
        this.durationBehind = durationBehind;
    }

    @Nonnull
    public Optional<ChannelPosition> getChannelPosition() {
        return Optional.ofNullable(channelPosition);
    }

    @Nonnull
    public String getShardPosition() {
        return channelPosition.positionOf(getShardName());
    }

    @Nonnull
    public String getShardName() {
        if (channelPosition != null && channelPosition.shards().size() == 1) {
            //noinspection ConstantConditions
            return getFirst(channelPosition.shards(), "");
        } else {
            return "";
        }
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
        return Objects.equals(channelPosition, header.channelPosition) &&
                Objects.equals(arrivalTimestamp, header.arrivalTimestamp) &&
                Objects.equals(durationBehind, header.durationBehind);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelPosition, arrivalTimestamp, durationBehind);
    }

    @Override
    public String toString() {
        return "Header{" +
                "channelPosition='" + channelPosition + '\'' +
                ", arrivalTimestamp=" + arrivalTimestamp +
                ", durationBehind=" + durationBehind +
                '}';
    }
}
