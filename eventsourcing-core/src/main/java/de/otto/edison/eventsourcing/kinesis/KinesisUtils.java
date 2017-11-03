package de.otto.edison.eventsourcing.kinesis;

import software.amazon.awssdk.AmazonServiceException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities to create and delete Amazon Kinesis streams.
 */
public class KinesisUtils {

    public static Map<String, String> getStartHashKeysForShards(KinesisClient kinesisClient, String streamName) {
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(streamName)
                .build();
        try {
            return kinesisClient.describeStream(describeStreamRequest).streamDescription()
                    .shards()
                    .stream()
                    .filter(KinesisUtils::isShardOpen)
                    .collect(Collectors.toMap(Shard::shardId, shard -> shard.hashKeyRange().startingHashKey()));
        } catch (AmazonServiceException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Shard> retrieveAllOpenShards(KinesisClient kinesisClient, String streamName) {
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
            shardList.addAll(describeStreamResult.streamDescription().shards().stream().filter(KinesisUtils::isShardOpen).collect(Collectors.toList()));

            if (describeStreamResult.streamDescription().hasMoreShards() && !shardList.isEmpty()) {
                exclusiveStartShardId = shardList.get(shardList.size() - 1).shardId();
            } else {
                exclusiveStartShardId = null;
            }

        } while (exclusiveStartShardId != null);
        return shardList;
    }

    private static boolean isShardOpen(Shard shard) {
        return shard.sequenceNumberRange().endingSequenceNumber() == null;
    }

}