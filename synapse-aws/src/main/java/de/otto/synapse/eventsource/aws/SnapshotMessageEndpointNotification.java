package de.otto.synapse.eventsource.aws;

import de.otto.synapse.info.MessageEndpointNotification;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A special {@link MessageEndpointNotification} that additionally holds the timestamp of snapshot creation.
 *
 * @deprecated don't rely on this event as the timestamp of snapshot creation should be obtained
 *             by the event message's metadata in the future
 */
@Deprecated
public class SnapshotMessageEndpointNotification extends MessageEndpointNotification {

    private final Instant snapshotTimestamp;

    private SnapshotMessageEndpointNotification(Builder builder) {
        super(builder);
        this.snapshotTimestamp = builder.snapshotTimestamp;
    }

    public Optional<Instant> getSnapshotTimestamp() {
        return Optional.ofNullable(snapshotTimestamp);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        SnapshotMessageEndpointNotification that = (SnapshotMessageEndpointNotification) o;
        return Objects.equals(snapshotTimestamp, that.snapshotTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), snapshotTimestamp);
    }

    @Override
    public String toString() {
        return "SnapshotMessageEndpointNotification{" +
                "snapshotTimestamp=" + snapshotTimestamp +
                ", channelPosition=" + getChannelPosition() +
                ", channelName='" + getChannelName() + '\'' +
                ", status=" + getStatus() +
                ", message='" + getMessage() + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }


    public static final class Builder extends MessageEndpointNotification.Builder {

        private Instant snapshotTimestamp;

        private Builder() {
        }

        public Builder withSnapshotTimestamp(Instant snapshotTimestamp) {
            this.snapshotTimestamp = snapshotTimestamp;
            return this;
        }

        public SnapshotMessageEndpointNotification build() {
            return new SnapshotMessageEndpointNotification(this);
        }
    }

}
