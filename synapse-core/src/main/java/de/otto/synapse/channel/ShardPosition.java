package de.otto.synapse.channel;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ShardPosition implements Serializable {
    private final String shardName;
    private final String position;
    private final StartFrom startFrom;

    private ShardPosition(final @Nonnull String shardName, final @Nonnull String position) {
        this.shardName = requireNonNull(shardName);
        this.position = requireNonNull(position);
        this.startFrom = StartFrom.POSITION;
    }

    private ShardPosition(final @Nonnull String shardName) {
        this.shardName = requireNonNull(shardName);
        this.position = "";
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

    public String shardName() {
        return shardName;
    }

    @Nonnull
    public String position() {
        return position;
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
                startFrom == that.startFrom;
    }

    @Override
    public int hashCode() {

        return Objects.hash(shardName, position, startFrom);
    }

    @Override
    public String toString() {
        return "ShardPosition{" +
                "shardName='" + shardName + '\'' +
                ", position='" + position + '\'' +
                ", startFrom=" + startFrom +
                '}';
    }
}
