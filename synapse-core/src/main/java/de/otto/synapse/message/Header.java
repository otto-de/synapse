package de.otto.synapse.message;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * The Header of a {@link Message}.
 *
 * <p>
 * <img src="http://www.enterpriseintegrationpatterns.com/img/MessageSolution.gif" alt="Message">
 * </p>
 *
 */
public class Header  {
    // TODO: Header extends ImmutableMultimap<String, Object>

    private static final Header EMPTY_HEADER = new Header("", Instant.MIN, null);

    public static Header emptyHeader() {
        return EMPTY_HEADER;
    }

    public static Header responseHeader(final String sequenceNumber,
                                        final Instant approximateArrivalTimestamp,
                                        final Duration durationBehind) {
        return new Header(
                sequenceNumber,
                approximateArrivalTimestamp,
                durationBehind);
    }

    public static Header responseHeader(final String sequenceNumber,
                                        final Instant approximateArrivalTimestamp) {
        return new Header(
                sequenceNumber,
                approximateArrivalTimestamp,
                null);
    }

    // TODO sequenceNumber -> StreamPosition
    private final String sequenceNumber;
    private final Instant arrivalTimestamp;
    private final Duration durationBehind;


    private Header(final String sequenceNumber,
                   final Instant approximateArrivalTimestamp,
                   final Duration durationBehind) {
        this.sequenceNumber = sequenceNumber;
        this.arrivalTimestamp = approximateArrivalTimestamp;
        this.durationBehind = durationBehind;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public Instant getArrivalTimestamp() {
        return arrivalTimestamp;
    }

    /**
     * Returns the approx. duration of this event behind the latest event in the event source.
     *
     * @return Duration
     */
    public Optional<Duration> getDurationBehind() {
        return Optional.ofNullable(durationBehind);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Header header = (Header) o;
        return Objects.equals(sequenceNumber, header.sequenceNumber) &&
                Objects.equals(arrivalTimestamp, header.arrivalTimestamp) &&
                Objects.equals(durationBehind, header.durationBehind);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequenceNumber, arrivalTimestamp, durationBehind);
    }

    @Override
    public String toString() {
        return "Header{" +
                "sequenceNumber='" + sequenceNumber + '\'' +
                ", arrivalTimestamp=" + arrivalTimestamp +
                ", durationBehind=" + durationBehind +
                '}';
    }
}
