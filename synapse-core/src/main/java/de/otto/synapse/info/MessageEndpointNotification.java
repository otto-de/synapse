package de.otto.synapse.info;

import de.otto.synapse.channel.ChannelPosition;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class MessageEndpointNotification {

    private ChannelPosition channelPosition;
    private String channelName;
    private MessageEndpointStatus status;
    private String message;

    protected MessageEndpointNotification(Builder builder) {
        channelPosition = requireNonNull(builder.channelPosition);
        channelName = requireNonNull(builder.channelName);
        status = requireNonNull(builder.status);
        message = requireNonNull(builder.message);
    }

    public ChannelPosition getChannelPosition() {
        return channelPosition;
    }

    public String getChannelName() {
        return channelName;
    }

    public MessageEndpointStatus getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageEndpointNotification that = (MessageEndpointNotification) o;
        return Objects.equals(channelPosition, that.channelPosition) &&
                Objects.equals(channelName, that.channelName) &&
                status == that.status &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {

        return Objects.hash(channelPosition, channelName, status, message);
    }

    @Override
    public String toString() {
        return "MessageEndpointNotification{" +
                "channelPosition=" + channelPosition +
                ", channelName='" + channelName + '\'' +
                ", status=" + status +
                ", message='" + message + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(MessageEndpointNotification copy) {
        Builder builder = new Builder();
        builder.channelPosition = copy.getChannelPosition();
        builder.channelName = copy.getChannelName();
        builder.status = copy.getStatus();
        return builder;
    }

    public static class Builder {
        private ChannelPosition channelPosition = ChannelPosition.fromHorizon();
        private String channelName = "";
        private MessageEndpointStatus status;
        private String message = "";

        protected Builder() {
        }

        public Builder withChannelPosition(ChannelPosition val) {
            channelPosition = val;
            return this;
        }

        public Builder withChannelName(String val) {
            channelName = val;
            return this;
        }

        public Builder withStatus(MessageEndpointStatus val) {
            status = val;
            return this;
        }

        public Builder withMessage(String msg) {
            message = msg;
            return this;
        }

        public MessageEndpointNotification build() {
            return new MessageEndpointNotification(this);
        }
    }
}
