package de.otto.synapse.endpoint.receiver.kinesis;

import java.util.Objects;

public class KinesisShardInfo {
    private final String shardName;
    private final boolean open;

    public KinesisShardInfo(final String shardName, final boolean open) {
        this.shardName = shardName;
        this.open = open;
    }

    public String getShardName() {
        return shardName;
    }

    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KinesisShardInfo that = (KinesisShardInfo) o;
        return open == that.open &&
                Objects.equals(shardName, that.shardName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardName, open);
    }

    @Override
    public String toString() {
        return "KinesisShardInfo{" +
                "shardName='" + shardName + '\'' +
                ", open=" + open +
                '}';
    }
}
