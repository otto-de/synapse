package de.otto.synapse.channel;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

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
        this.startFrom = position.isEmpty() ? StartFrom.HORIZON : StartFrom.POSITION;
    }

    private ShardPosition(final @Nonnull String shardName, final @Nonnull Instant timestamp) {
        this.shardName = requireNonNull(shardName);
        this.position = "";
        this.timestamp = requireNonNull(timestamp);
        this.startFrom = StartFrom.TIMESTAMP;
    }

    private ShardPosition(final String shardName, final String position, final Instant timestamp, final StartFrom startFrom) {
        this.shardName = requireNonNull(shardName);
        this.position = position;
        this.timestamp = timestamp;
        this.startFrom = startFrom;
    }

    @Nonnull
    public static ShardPosition fromHorizon(final @Nonnull String shardName) {
        return new ShardPosition(shardName, "");
    }

    @Nonnull
    public static ShardPosition fromPosition(final @Nonnull String shardName,
                                             final @Nonnull String position) {
        return new ShardPosition(shardName, position);
    }

    @Nonnull
    public static ShardPosition atPosition(final @Nonnull String shardName,
                                             final @Nonnull String position) {
        return new ShardPosition(shardName, position, null, StartFrom.AT_POSITION);
    }

    @Nonnull
    public static ShardPosition fromTimestamp(final @Nonnull String shardName,
                                              final @Nonnull Instant timestamp) {
        return new ShardPosition(shardName, timestamp);
    }
    
    @Nonnull
    public static ShardPosition fromPositionAndTimestamp(final @Nonnull String shardName,
                                                         final @Nonnull String position,
                                                         final @Nonnull Instant timestamp) {
        return new ShardPosition(shardName, position, timestamp, StartFrom.POSITION);
    }

    @Nonnull
    @JsonProperty
    public String shardName() {
        return shardName;
    }

    @Nullable
    @JsonProperty
    public String position() {
        return position;
    }

    @Nullable
    @JsonProperty
    public Instant timestamp() {
        return timestamp;
    }

    @Nonnull
    @JsonProperty
    public StartFrom startFrom() {
        return startFrom;
    }

    @Override
    public String toString() {
        return "ShardPosition{" +
                "shardName='" + shardName + '\'' +
                ", position='" + position + '\'' +
                ", startFrom=" + startFrom +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardPosition that = (ShardPosition) o;
        return Objects.equals(shardName, that.shardName) &&
                Objects.equals(position, that.position) &&
                startFrom == that.startFrom &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {

        return Objects.hash(shardName, position, startFrom, timestamp);
    }

}
