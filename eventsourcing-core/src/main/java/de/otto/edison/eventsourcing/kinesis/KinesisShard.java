package de.otto.edison.eventsourcing.kinesis;

public class KinesisShard {
    private final String shardId;

    public KinesisShard(String shardId) {
        this.shardId = shardId;
    }

    public String getShardId() {
        return shardId;
    }
}
