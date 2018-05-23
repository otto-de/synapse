package de.otto.synapse.channel;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;

public final class ShardPosition implements Serializable {
    private final static Duration MAX_DURATION = ofMillis(Long.MAX_VALUE);

    private final String shardName;
    private final Duration durationBehind;
    private final String position;
    private final StartFrom startFrom;
    private final Instant timestamp;

    private ShardPosition(final @Nonnull String shardName, final @Nonnull Duration durationBehind, final @Nonnull String position) {
        this.shardName = requireNonNull(shardName);
        this.durationBehind = requireNonNull(durationBehind);
        this.position = requireNonNull(position);
        this.timestamp = null;
        this.startFrom = position.isEmpty() ? StartFrom.HORIZON : StartFrom.POSITION;
    }

    private ShardPosition(final @Nonnull String shardName, final @Nonnull Duration durationBehind, final @Nonnull Instant timestamp) {
        this.shardName = requireNonNull(shardName);
        this.durationBehind = requireNonNull(durationBehind);
        this.position = "";
        this.timestamp = requireNonNull(timestamp);
        this.startFrom = StartFrom.TIMESTAMP;
    }

    public ShardPosition withDurationBehind(final @Nonnull Duration durationBehind) {
        return timestamp == null
                ? new ShardPosition(shardName, durationBehind, position)
                : new ShardPosition(shardName, durationBehind, timestamp);
    }

    @Nonnull
    public static ShardPosition fromHorizon(final @Nonnull String shardName) {
        return new ShardPosition(shardName, MAX_DURATION, "");
    }

    @Nonnull
    public static ShardPosition fromHorizon(final @Nonnull String shardName, final @Nonnull Duration durationBehind) {
        return new ShardPosition(shardName, durationBehind, "");
    }

    @Nonnull
    public static ShardPosition fromPosition(final @Nonnull String shardName,
                                             final @Nonnull Duration durationBehind,
                                             final @Nonnull String position) {
        return new ShardPosition(shardName, durationBehind, position);
    }

    @Nonnull
    public static ShardPosition fromTimestamp(final @Nonnull String shardName,
                                              final @Nonnull Duration durationBehind,
                                              final @Nonnull Instant timestamp) {
        return new ShardPosition(shardName, durationBehind, timestamp);
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

    @Nonnull
    public Duration getDurationBehind() {
        return durationBehind;
    }

    @Override
    public String toString() {
        return "ShardPosition{" +
                "shardName='" + shardName + '\'' +
                ", durationBehind=" + durationBehind +
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
                Objects.equals(durationBehind, that.durationBehind) &&
                Objects.equals(position, that.position) &&
                startFrom == that.startFrom &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {

        return Objects.hash(shardName, durationBehind, position, startFrom, timestamp);
    }

}
