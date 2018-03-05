package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;

import java.util.Objects;

public class EventSourceNotification {

    public enum Status {
        STARTED,
        FAILED,
        FINISHED
    }

    private String eventSourceName;
    private ChannelPosition channelPosition;
    private String streamName;
    private Status status;
    private String message;

    private EventSourceNotification(Builder builder) {
        eventSourceName = builder.eventSourceName;
        channelPosition = builder.channelPosition;
        streamName = builder.streamName;
        status = builder.status;
        message = builder.message;
    }

    public String getEventSourceName() {
        return eventSourceName;
    }

    public ChannelPosition getChannelPosition() {
        return channelPosition;
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
                Objects.equals(channelPosition, that.channelPosition) &&
                Objects.equals(streamName, that.streamName) &&
                status == that.status &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventSourceName, channelPosition, streamName, status, message);
    }

    @Override
    public String toString() {
        return "EventSourceNotification{" +
                "eventSourceName='" + eventSourceName + '\'' +
                ", streamPosition=" + channelPosition +
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
        builder.channelPosition = copy.getChannelPosition();
        builder.streamName = copy.getStreamName();
        builder.status = copy.getStatus();
        return builder;
    }

    public static final class Builder {
        private String eventSourceName;
        private ChannelPosition channelPosition;
        private String streamName;
        private Status status;
        private String message;

        private Builder() {
        }

        public Builder withEventSourceName(String val) {
            eventSourceName = val;
            return this;
        }

        public Builder withStreamPosition(ChannelPosition val) {
            channelPosition = val;
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
