package de.otto.edison.eventsourcing.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import static java.lang.String.format;

public class KinesisUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisUtils.class);

    private KinesisClient kinesisClient;

    public KinesisUtils(KinesisClient kinesisClient) {
        this.kinesisClient = kinesisClient;
    }

    public GetRecordsResponse getRecords(String shardIterator) {
        return kinesisClient.getRecords(GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .build());
    }

    public String getShardIterator(String streamName, String shardId, String shardPosition) {
        GetShardIteratorResponse shardIterator;
        try {
            shardIterator = kinesisClient.getShardIterator(buildIteratorShardRequest(streamName, shardId, shardPosition));
        } catch (final InvalidArgumentException e) {
            LOG.error(format("invalidShardSequenceNumber in Snapshot %s/%s - reading from HORIZON", streamName, shardId));
            shardIterator = kinesisClient.getShardIterator(buildIteratorShardRequest(streamName, shardId, "0"));
        }
        return shardIterator.shardIterator();
    }

    private GetShardIteratorRequest buildIteratorShardRequest(String streamName, String shardId, String shardPosition) {
        GetShardIteratorRequest.Builder shardRequestBuilder = GetShardIteratorRequest
                .builder()
                .shardId(shardId)
                .streamName(streamName);

        if (shardPosition == null || shardPosition.equals("0")) {
            shardRequestBuilder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        } else {
            shardRequestBuilder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            shardRequestBuilder.startingSequenceNumber(shardPosition);
        }

        return shardRequestBuilder.build();
    }
}