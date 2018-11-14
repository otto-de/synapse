package de.otto.synapse.endpoint.receiver.kinesis;

import software.amazon.kinesis.checkpoint.Checkpoint;
import software.amazon.kinesis.exceptions.KinesisClientLibException;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

public class KCLCheckpointer implements Checkpointer {

    @Override
    public void setCheckpoint(String shardId, ExtendedSequenceNumber checkpointValue, String concurrencyToken) throws KinesisClientLibException {

    }

    @Override
    public ExtendedSequenceNumber getCheckpoint(String shardId) throws KinesisClientLibException {
        return null;
    }

    @Override
    public Checkpoint getCheckpointObject(String shardId) throws KinesisClientLibException {
        return null;
    }

    @Override
    public void prepareCheckpoint(String shardId, ExtendedSequenceNumber pendingCheckpoint, String concurrencyToken) throws KinesisClientLibException {

    }

    @Override
    public void operation(String operation) {

    }

    @Override
    public String operation() {
        return null;
    }
}
