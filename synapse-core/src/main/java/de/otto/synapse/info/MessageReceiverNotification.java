package de.otto.synapse.info;

import de.otto.synapse.channel.ChannelDurationBehind;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MessageReceiverNotification {

    private String channelName;
    private MessageReceiverStatus status;
    private ChannelDurationBehind channelDurationBehind;
    private String message;

    protected MessageReceiverNotification(Builder builder) {
        channelDurationBehind = builder.channelDurationBehind;
        channelName = requireNonNull(builder.channelName);
        status = requireNonNull(builder.status);
        message = requireNonNull(builder.message);
    }

    public Optional<ChannelDurationBehind> getChannelDurationBehind() {
        return Optional.ofNullable(channelDurationBehind);
    }

    public String getChannelName() {
        return channelName;
    }

    public MessageReceiverStatus getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageReceiverNotification that = (MessageReceiverNotification) o;
        return Objects.equals(channelName, that.channelName) &&
                status == that.status &&
                Objects.equals(channelDurationBehind, that.channelDurationBehind) &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {

        return Objects.hash(channelName, status, channelDurationBehind, message);
    }

    @Override
    public String toString() {
        return "MessageReceiverNotification{" +
                "channelName='" + channelName + '\'' +
                ", status=" + status +
                ", channelDurationBehind=" + channelDurationBehind +
                ", message='" + message + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(MessageReceiverNotification copy) {
        Builder builder = new Builder();
        builder.channelDurationBehind= copy.getChannelDurationBehind().orElse(null);
        builder.channelName = copy.getChannelName();
        builder.status = copy.getStatus();
        return builder;
    }

    public static class Builder {
        private ChannelDurationBehind channelDurationBehind = null;
        private String channelName = "";
        private MessageReceiverStatus status;
        private String message = "";

        protected Builder() {
        }

        public Builder withChannelDurationBehind(ChannelDurationBehind val) {
            channelDurationBehind = val;
            return this;
        }

        public Builder withChannelName(String val) {
            channelName = val;
            return this;
        }

        public Builder withStatus(MessageReceiverStatus val) {
            status = val;
            return this;
        }

        public Builder withMessage(String msg) {
            message = msg;
            return this;
        }

        public MessageReceiverNotification build() {
            return new MessageReceiverNotification(this);
        }
    }
}
