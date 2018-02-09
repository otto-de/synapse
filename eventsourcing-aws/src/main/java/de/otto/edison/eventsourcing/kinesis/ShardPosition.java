package de.otto.edison.eventsourcing.kinesis;

import java.util.Objects;

class ShardPosition {

    private String shardId;
    private String sequenceNumber;

    public ShardPosition(String shardId, String sequenceNumber) {
        this.shardId = shardId;
        this.sequenceNumber = sequenceNumber;
    }

    private ShardPosition(Builder builder) {
        setSequenceNumber(builder.sequenceNumber);
        setShardId(builder.shardId);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(String sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardPosition that = (ShardPosition) o;
        return Objects.equals(sequenceNumber, that.sequenceNumber) &&
                Objects.equals(shardId, that.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequenceNumber, shardId);
    }

    @Override
    public String toString() {
        return "SequenceNumberOfShard{" +
                "sequenceNumber='" + sequenceNumber + '\'' +
                ", shardId='" + shardId + '\'' +
                '}';
    }


    public static final class Builder {
        private String sequenceNumber;
        private String shardId;

        private Builder() {
        }

        public Builder withSequenceNumber(String val) {
            sequenceNumber = val;
            return this;
        }

        public Builder withShardId(String val) {
            shardId = val;
            return this;
        }

        public ShardPosition build() {
            return new ShardPosition(this);
        }
    }
}
