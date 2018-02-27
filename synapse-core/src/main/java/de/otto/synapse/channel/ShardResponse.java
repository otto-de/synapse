package de.otto.synapse.channel;

import java.util.Objects;

public final class ShardResponse {

    private final Status status;
    private final ShardPosition shardPosition;

    private ShardResponse(final Status status,
                         final ShardPosition shardPosition) {
        this.status = status;
        this.shardPosition = shardPosition;
    }

    public static ShardResponse of(final Status status,
                                   final ShardPosition shardPosition) {
        return new ShardResponse(status, shardPosition);
    }

    public Status getStatus() {
        return status;
    }

    public ShardPosition getShardPosition() {
        return shardPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardResponse that = (ShardResponse) o;
        return status == that.status &&
                Objects.equals(shardPosition, that.shardPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, shardPosition);
    }

    @Override
    public String toString() {
        return "ShardResponse{" +
                "status=" + status +
                ", shardPosition=" + shardPosition +
                '}';
    }
}
