package de.otto.edison.eventsourcing.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.AmazonServiceException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Utilities to create and delete Amazon Kinesis streams.
 */
@Component
public class KinesisUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisUtils.class);

    private KinesisClient kinesisClient;

    @Autowired
    public KinesisUtils(KinesisClient kinesisClient) {
        this.kinesisClient = kinesisClient;
    }

    public Map<String, String> getStartHashKeysForShards(String streamName) {
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(streamName)
                .build();
        try {
            return kinesisClient.describeStream(describeStreamRequest).streamDescription()
                    .shards()
                    .stream()
                    .filter(this::isShardOpen)
                    .collect(Collectors.toMap(Shard::shardId, shard -> shard.hashKeyRange().startingHashKey()));
        } catch (AmazonServiceException e) {
            throw new RuntimeException(e);
        }
    }

    public GetRecordsResponse getRecords(String shardIterator) {
        return kinesisClient.getRecords(GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .build());
    }

    public List<Shard> retrieveAllOpenShards(String streamName) {
        List<Shard> shardList = new ArrayList<>();

        DescribeStreamRequest describeStreamRequest;
        String exclusiveStartShardId = null;

        do {
            describeStreamRequest = DescribeStreamRequest
                    .builder()
                    .streamName(streamName)
                    .exclusiveStartShardId(exclusiveStartShardId)
                    .limit(10)
                    .build();

            DescribeStreamResponse describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
            shardList.addAll(describeStreamResult.streamDescription().shards().stream().filter(this::isShardOpen).collect(Collectors.toList()));

            if (describeStreamResult.streamDescription().hasMoreShards() && !shardList.isEmpty()) {
                exclusiveStartShardId = shardList.get(shardList.size() - 1).shardId();
            } else {
                exclusiveStartShardId = null;
            }

        } while (exclusiveStartShardId != null);
        return shardList;
    }

    private boolean isShardOpen(Shard shard) {
        return shard.sequenceNumberRange().endingSequenceNumber() == null;
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