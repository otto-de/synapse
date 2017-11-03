package de.otto.edison.eventsourcing.consumer;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public class Event<T> {

    public static <T> Event<T> event(final String streamName,
                                     final String key,
                                     final T payload,
                                     final String sequenceNumber,
                                     final Instant approximateArrivalTimestamp) {
        return new Event<>(
                streamName,
                key,
                payload,
                sequenceNumber,
                approximateArrivalTimestamp, null);
    }

    public static <T> Event<T> event(final String streamName,
                                     final String key,
                                     final T payload,
                                     final String sequenceNumber,
                                     final Instant approximateArrivalTimestamp,
                                     final Duration durationBehind) {
        return new Event<>(
                streamName,
                key,
                payload,
                sequenceNumber,
                approximateArrivalTimestamp,
                durationBehind);
    }

    private final String key;
    private final T payload;
    private final String sequenceNumber;
    private final Instant arrivalTimestamp;
    private final String streamName;
    private final Duration durationBehind;


    protected Event(final String streamName,
                  final String key,
                  final T payload,
                  final String sequenceNumber,
                  final Instant approximateArrivalTimestamp,
                  final Duration durationBehind) {
        this.key = key;
        this.payload = payload;
        this.sequenceNumber = sequenceNumber;
        this.arrivalTimestamp = approximateArrivalTimestamp;
        this.streamName = streamName;
        this.durationBehind = durationBehind;
    }

    public String key() {
        return key;
    }

    public T payload() {
        return payload;
    }

    public String streamName() {
        return streamName;
    }

    public String sequenceNumber() {
        return sequenceNumber;
    }

    public Instant arrivalTimestamp() {
        return arrivalTimestamp;
    }

    /**
     * Returns the approx. duration of this event behind the latest event in the event source.
     *
     * @return Duration
     */
    @Nonnull
    public Optional<Duration> durationBehind() {
        return Optional.ofNullable(durationBehind);
    }

}
