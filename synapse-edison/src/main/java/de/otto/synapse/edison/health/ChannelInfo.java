package de.otto.synapse.edison.health;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ChannelInfo {

    public enum Status {
        BEHIND, HEAD
    }

    private final String message;
    private final Status status;

    public ChannelInfo(ChannelInfoBuilder builder) {
        this.message = requireNonNull(builder.message);
        this.status = requireNonNull(builder.status);
    }

    public String getMessage() {
        return message;
    }

    public Status getStatus() {
        return status;
    }

    public static ChannelInfoBuilder startupDetailBuilder() {
        return new ChannelInfoBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChannelInfo that = (ChannelInfo) o;
        return Objects.equals(message, that.message) &&
                status == that.status;
    }

    @Override
    public int hashCode() {

        return Objects.hash(message, status);
    }

    @Override
    public String toString() {
        return "ChannelInfo{" +
                "message='" + message + '\'' +
                ", status=" + status +
                '}';
    }

    public static final class ChannelInfoBuilder {

        private String message;
        private ChannelInfo.Status status;

        private ChannelInfoBuilder() {
        }

        public ChannelInfoBuilder withMessage(String message) {
            this.message = message;
            return this;
        }

        public ChannelInfoBuilder withStatus(ChannelInfo.Status status) {
            this.status = status;
            return this;
        }

        public ChannelInfo build() {
            return new ChannelInfo(this);
        }
    }

}
