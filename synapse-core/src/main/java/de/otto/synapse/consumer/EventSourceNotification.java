package de.otto.synapse.consumer;

import java.util.Objects;

public class EventSourceNotification {

    public enum Status {
        STARTED,
        FAILED,
        FINISHED
    }

    private String eventSourceName;
    private StreamPosition streamPosition;
    private String streamName;
    private Status status;
    private String message;

    private EventSourceNotification(Builder builder) {
        eventSourceName = builder.eventSourceName;
        streamPosition = builder.streamPosition;
        streamName = builder.streamName;
        status = builder.status;
        message = builder.message;
    }

    public String getEventSourceName() {
        return eventSourceName;
    }

    public StreamPosition getStreamPosition() {
        return streamPosition;
    }

    public String getStreamName() {
        return streamName;
    }

    public Status getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventSourceNotification that = (EventSourceNotification) o;
        return Objects.equals(eventSourceName, that.eventSourceName) &&
                Objects.equals(streamPosition, that.streamPosition) &&
                Objects.equals(streamName, that.streamName) &&
                status == that.status &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventSourceName, streamPosition, streamName, status, message);
    }

    @Override
    public String toString() {
        return "EventSourceNotification{" +
                "eventSourceName='" + eventSourceName + '\'' +
                ", streamPosition=" + streamPosition +
                ", streamName='" + streamName + '\'' +
                ", status=" + status +
                ", message='" + message + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(EventSourceNotification copy) {
        Builder builder = new Builder();
        builder.streamPosition = copy.getStreamPosition();
        builder.streamName = copy.getStreamName();
        builder.status = copy.getStatus();
        return builder;
    }

    public static final class Builder {
        private String eventSourceName;
        private StreamPosition streamPosition;
        private String streamName;
        private Status status;
        private String message;

        private Builder() {
        }

        public Builder withEventSourceName(String val) {
            eventSourceName = val;
            return this;
        }

        public Builder withStreamPosition(StreamPosition val) {
            streamPosition = val;
            return this;
        }

        public Builder withStreamName(String val) {
            streamName = val;
            return this;
        }

        public Builder withStatus(Status val) {
            status = val;
            return this;
        }

        public Builder withMessage(String msg) {
            message = msg;
            return this;
        }

        public EventSourceNotification build() {
            return new EventSourceNotification(this);
        }
    }
}
