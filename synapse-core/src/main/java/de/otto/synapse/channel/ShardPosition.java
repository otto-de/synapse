package de.otto.synapse.channel;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ShardPosition implements Serializable {
    private final String shardName;
    private final String position;
    private final StartFrom startFrom;
    private final Instant timestamp;

    private ShardPosition(final @Nonnull String shardName, final @Nonnull String position) {
        this.shardName = requireNonNull(shardName);
        this.position = requireNonNull(position);
        this.timestamp = null;
        this.startFrom = StartFrom.POSITION;
    }

    private ShardPosition(final @Nonnull String shardName, final @Nonnull Instant timestamp) {
        this.shardName = requireNonNull(shardName);
        this.position = "";
        this.timestamp = requireNonNull(timestamp);
        this.startFrom = StartFrom.TIMESTAMP;
    }

    private ShardPosition(final @Nonnull String shardName) {
        this.shardName = requireNonNull(shardName);
        this.position = "";
        this.timestamp = null;
        this.startFrom = StartFrom.HORIZON;
    }

    @Nonnull
    public static ShardPosition fromHorizon(final @Nonnull String shardName) {
        return new ShardPosition(shardName);
    }

    @Nonnull
    public static ShardPosition fromPosition(final @Nonnull String shardName,
                                             final @Nonnull String position) {
        return new ShardPosition(shardName, position);
    }

    @Nonnull
    public static ShardPosition fromTimestamp(final @Nonnull String shardName,
                                             final @Nonnull Instant timestamp) {
        return new ShardPosition(shardName, timestamp);
    }

    public String shardName() {
        return shardName;
    }

    @Nonnull
    public String position() {
        return position;
    }

    @Nonnull
    public Instant timestamp() {
        return timestamp;
    }

    @Nonnull
    public StartFrom startFrom() {
        return startFrom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardPosition that = (ShardPosition) o;
        return Objects.equals(shardName, that.shardName) &&
                Objects.equals(position, that.position) &&
                Objects.equals(timestamp, that.timestamp) &&
                startFrom == that.startFrom;
    }

    @Override
    public int hashCode() {

        return Objects.hash(shardName, position, timestamp, startFrom);
    }

    @Override
    public String toString() {
        return "ShardPosition{" +
                "shardName='" + shardName + '\'' +
                ", position='" + position + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", startFrom=" + startFrom +
                '}';
    }
}
