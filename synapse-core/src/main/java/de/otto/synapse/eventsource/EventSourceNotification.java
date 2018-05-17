package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;

import java.time.Duration;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class EventSourceNotification {

    public enum Status {
        STARTED,
        RUNNING,
        FAILED,
        FINISHED
    }

    private String eventSourceName;
    private ChannelPosition channelPosition;
    private String channelName;
    private Status status;
    private String message;

    protected EventSourceNotification(Builder builder) {
        eventSourceName = requireNonNull(builder.eventSourceName);
        channelPosition = requireNonNull(builder.channelPosition);
        channelName = requireNonNull(builder.channelName);
        status = requireNonNull(builder.status);
        message = requireNonNull(builder.message);
    }

    public String getEventSourceName() {
        return eventSourceName;
    }

    public ChannelPosition getChannelPosition() {
        return channelPosition;
    }

    public String getChannelName() {
        return channelName;
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
                Objects.equals(channelName, that.channelName) &&
                status == that.status &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {

        return Objects.hash(eventSourceName, channelPosition, channelName, status, message);
    }

    @Override
    public String toString() {
        return "EventSourceNotification{" +
                "eventSourceName='" + eventSourceName + '\'' +
                ", channelPosition=" + channelPosition +
                ", channelName='" + channelName + '\'' +
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
        builder.channelName = copy.getChannelName();
        builder.status = copy.getStatus();
        return builder;
    }

    public static class Builder {
        private String eventSourceName;
        private ChannelPosition channelPosition = ChannelPosition.fromHorizon();
        private String channelName = "";
        private Status status;
        private String message = "";

        protected Builder() {
        }

        public Builder withEventSourceName(String val) {
            eventSourceName = val;
            return this;
        }

        public Builder withChannelPosition(ChannelPosition val) {
            channelPosition = val;
            return this;
        }

        public Builder withChannelName(String val) {
            channelName = val;
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
