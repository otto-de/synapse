package de.otto.synapse.info;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SnapshotReaderNotification {

    private String channelName;
    private SnapshotReaderStatus status;
    private String message;
    private Instant snapshotTimestamp;

    protected SnapshotReaderNotification(Builder builder) {
        channelName = requireNonNull(builder.channelName);
        status = requireNonNull(builder.status);
        message = requireNonNull(builder.message);
        snapshotTimestamp = builder.snapshotTimestamp;
    }

    @Nonnull
    public String getChannelName() {
        return channelName;
    }

    @Nonnull
    public SnapshotReaderStatus getStatus() {
        return status;
    }

    @Nonnull
    public String getMessage() {
        return message;
    }

    @Nullable
    public Instant getSnapshotTimestamp() {
        return snapshotTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotReaderNotification that = (SnapshotReaderNotification) o;
        return Objects.equals(channelName, that.channelName) &&
                status == that.status &&
                Objects.equals(message, that.message) &&
                Objects.equals(snapshotTimestamp, that.snapshotTimestamp);
    }

    @Override
    public int hashCode() {

        return Objects.hash(channelName, status, message, snapshotTimestamp);
    }

    @Override
    public String toString() {
        return "SnapshotReaderNotification{" +
                "channelName='" + channelName + '\'' +
                ", status=" + status +
                ", message='" + message + '\'' +
                ", snapshotTimestamp=" + snapshotTimestamp +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(SnapshotReaderNotification copy) {
        Builder builder = new Builder();
        builder.channelName = copy.getChannelName();
        builder.status = copy.getStatus();
        return builder;
    }

    public static class Builder {
        private String channelName = "";
        private SnapshotReaderStatus status;
        private String message = "";
        private Instant snapshotTimestamp;

        protected Builder() {
        }

        public Builder withChannelName(String val) {
            channelName = val;
            return this;
        }

        public Builder withStatus(SnapshotReaderStatus val) {
            status = val;
            return this;
        }

        public Builder withMessage(String msg) {
            message = msg;
            return this;
        }

        public Builder withSnapshotTimestamp(final Instant snapshotTimestamp) {
            this.snapshotTimestamp = snapshotTimestamp;
            return this;
        }

        public SnapshotReaderNotification build() {
            return new SnapshotReaderNotification(this);
        }
    }
}
