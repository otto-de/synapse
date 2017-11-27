package de.otto.edison.eventsourcing.consumer;

import java.util.Objects;

public class EventSourceNotification {

    public enum Status {
        STARTED,
        FINISHED
    }

    private StreamPosition streamPosition;
    private EventSource eventSource;
    private Status status;

    private EventSourceNotification(Builder builder) {
        streamPosition = builder.streamPosition;
        eventSource = builder.eventSource;
        status = builder.status;
    }

    public StreamPosition getStreamPosition() {
        return streamPosition;
    }

    public EventSource getEventSource() {
        return eventSource;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventSourceNotification that = (EventSourceNotification) o;
        return Objects.equals(streamPosition, that.streamPosition) &&
                Objects.equals(eventSource, that.eventSource) &&
                status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamPosition, eventSource, status);
    }

    @Override
    public String toString() {
        return "EventSourceNotification{" +
                "streamPosition=" + streamPosition +
                ", eventSource=" + eventSource +
                ", status=" + status +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(EventSourceNotification copy) {
        Builder builder = new Builder();
        builder.streamPosition = copy.getStreamPosition();
        builder.eventSource = copy.getEventSource();
        builder.status = copy.getStatus();
        return builder;
    }

    public static final class Builder {
        private StreamPosition streamPosition;
        private EventSource eventSource;
        private Status status;

        private Builder() {
        }

        public Builder withStreamPosition(StreamPosition val) {
            streamPosition = val;
            return this;
        }

        public Builder withEventSource(EventSource val) {
            eventSource = val;
            return this;
        }

        public Builder withStatus(Status val) {
            status = val;
            return this;
        }

        public EventSourceNotification build() {
            return new EventSourceNotification(this);
        }
    }
}
