package de.otto.synapse.info;

import de.otto.synapse.channel.ChannelDurationBehind;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MessageReceiverEndpointInfo {

    private final String channelName;
    private final String message;
    private final MessageReceiverStatus status;
    private final Optional<ChannelDurationBehind> durationBehind;

    private MessageReceiverEndpointInfo(Builder builder) {
        this.message = requireNonNull(builder.message);
        this.status = requireNonNull(builder.status);
        this.channelName = requireNonNull(builder.channelName);
        this.durationBehind = Optional.ofNullable(builder.channelDurationBehind);
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

    public MessageReceiverStatus getStatus() {
        return status;
    }

    public Optional<ChannelDurationBehind> getDurationBehind() {
        return durationBehind;
    }

    @Override
    public String toString() {
        return "MessageReceiverEndpointInfo{" +
                "channelName='" + channelName + '\'' +
                ", message='" + message + '\'' +
                ", status=" + status +
                ", durationBehind=" + durationBehind +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageReceiverEndpointInfo that = (MessageReceiverEndpointInfo) o;
        return Objects.equals(channelName, that.channelName) &&
                Objects.equals(message, that.message) &&
                status == that.status &&
                Objects.equals(durationBehind, that.durationBehind);
    }

    @Override
    public int hashCode() {

        return Objects.hash(channelName, message, status, durationBehind);
    }

    public static Builder builder(MessageReceiverEndpointInfo info) {
        return new Builder().withMessage(info.message).withStatus(info.status).withChannelName(info.channelName);
    }

    public static final class Builder {

        private String channelName;
        private String message;
        private MessageReceiverStatus status;
        private ChannelDurationBehind channelDurationBehind;

        private Builder() {
        }

        public Builder withMessage(final String message) {
            this.message = message;
            return this;
        }

        public Builder withStatus(final MessageReceiverStatus status) {
            this.status = status;
            return this;
        }

        public Builder withChannelName(final String channelName) {
            this.channelName = channelName;
            return this;
        }

        public Builder withChannelDurationBehind(final ChannelDurationBehind durationBehind) {
            this.channelDurationBehind = durationBehind;
            return this;
        }

        public MessageReceiverEndpointInfo build() {
            return new MessageReceiverEndpointInfo(this);
        }
    }

}
