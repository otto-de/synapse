package de.otto.synapse.info;

import de.otto.synapse.channel.ChannelPosition;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MessageReceiverEndpointInfo {

    private final String channelName;
    private final String message;
    private final MessageEndpointStatus status;

    private final Optional<ChannelPosition> channelPosition;

    public MessageReceiverEndpointInfo(Builder builder) {
        this.message = requireNonNull(builder.message);
        this.status = requireNonNull(builder.status);
        this.channelName = requireNonNull(builder.channelName);
        this.channelPosition = Optional.ofNullable(builder.channelPosition);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getChannelName() {
        return channelName;
    }

    public String getMessage() {
        return message;
    }

    public MessageEndpointStatus getStatus() {
        return status;
    }

    public Optional<ChannelPosition> getChannelPosition() {
        return channelPosition;
    }

    @Override
    public String toString() {
        return "MessageReceiverEndpointInfo{" +
                "channelName='" + channelName + '\'' +
                ", message='" + message + '\'' +
                ", status=" + status +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageReceiverEndpointInfo that = (MessageReceiverEndpointInfo) o;
        return Objects.equals(channelName, that.channelName) &&
                Objects.equals(message, that.message) &&
                status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelName, message, status);
    }

    public static Builder builder(MessageReceiverEndpointInfo info) {
        return new Builder().withMessage(info.message).withStatus(info.status).withChannelName(info.channelName);
    }

    public static final class Builder {

        private String channelName;
        private String message;
        private MessageEndpointStatus status;
        private ChannelPosition channelPosition;

        private Builder() {
        }

        public Builder withMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder withStatus(MessageEndpointStatus status) {
            this.status = status;
            return this;
        }

        public Builder withChannelName(String channelName) {
            this.channelName = channelName;
            return this;
        }

        public Builder withChannelPosition(ChannelPosition channelPosition) {
            this.channelPosition = channelPosition;
            return this;
        }

        public MessageReceiverEndpointInfo build() {
            return new MessageReceiverEndpointInfo(this);
        }
    }

}
