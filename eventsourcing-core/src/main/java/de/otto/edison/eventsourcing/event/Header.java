package de.otto.edison.eventsourcing.event;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/*
 * TODO: extends ImmutableMultimap<String, Object>
 */
public class Header  {

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
