package de.otto.synapse.endpoint.receiver.kinesis;

import software.amazon.awssdk.services.kinesis.model.Record;

class RecordWithShard {

    private final String shardName;
    private final Record record;

    RecordWithShard(String shardName, Record record) {
        this.shardName = shardName;
        this.record = record;
    }

    public String getShardName() {
        return shardName;
    }

    public Record getRecord() {
        return record;
    }
}
