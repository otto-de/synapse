package de.otto.edison.eventsourcing.event;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public class Event<T> {

    public static <T> Event<T> event(final String key,
                                     final T payload,
                                     final String sequenceNumber,
                                     final Instant approximateArrivalTimestamp) {
        return new Event<>(
                EventBody.eventBody(key,payload),
                sequenceNumber,
                approximateArrivalTimestamp, null);
    }

    public static <T> Event<T> event(final String key,
                                     final T payload,
                                     final String sequenceNumber,
                                     final Instant approximateArrivalTimestamp,
                                     final Duration durationBehind) {
        return new Event<>(
                EventBody.eventBody(key,payload),
                sequenceNumber,
                approximateArrivalTimestamp,
                durationBehind);
    }

    public static <T> Event<T> event(final EventBody<T> eventBody,
                                     final String sequenceNumber,
                                     final Instant approximateArrivalTimestamp,
                                     final Duration durationBehind) {
        return new Event<>(
                eventBody,
                sequenceNumber,
                approximateArrivalTimestamp,
                durationBehind);
    }

    private final EventBody<T> eventBody;
    private final String sequenceNumber;
    private final Instant arrivalTimestamp;
    private final Duration durationBehind;


    protected Event(final EventBody<T> eventBody,
                    final String sequenceNumber,
                    final Instant approximateArrivalTimestamp,
                    final Duration durationBehind) {
        this.eventBody = eventBody;
        this.sequenceNumber = sequenceNumber;
        this.arrivalTimestamp = approximateArrivalTimestamp;
        this.durationBehind = durationBehind;
    }

    public EventBody<T> getEventBody() {
        return eventBody;
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
        Event<?> event = (Event<?>) o;
        return Objects.equals(eventBody, event.eventBody) &&
                Objects.equals(sequenceNumber, event.sequenceNumber) &&
                Objects.equals(arrivalTimestamp, event.arrivalTimestamp) &&
                Objects.equals(durationBehind, event.durationBehind);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventBody, sequenceNumber, arrivalTimestamp, durationBehind);
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventBody=" + eventBody +
                ", sequenceNumber='" + sequenceNumber + '\'' +
                ", arrivalTimestamp=" + arrivalTimestamp +
                ", durationBehind=" + durationBehind +
                '}';
    }
}
