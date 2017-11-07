package de.otto.edison.eventsourcing.kinesis;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class KinesisStream {

    private final KinesisClient kinesisClient;
    private final String streamName;

    public KinesisStream(KinesisClient kinesisClient, String streamName) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
    }

    public List<KinesisShard> retrieveAllOpenShards() {
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
            shardList.addAll(describeStreamResult.streamDescription().shards());

            if (describeStreamResult.streamDescription().hasMoreShards() && !shardList.isEmpty()) {
                exclusiveStartShardId = shardList.get(shardList.size() - 1).shardId();
            } else {
                exclusiveStartShardId = null;
            }

        } while (exclusiveStartShardId != null);
        return shardList.stream()
                .filter(this::isShardOpen)
                .map(shard -> new KinesisShard(shard.shardId()))
                .collect(toImmutableList());
    }

    private boolean isShardOpen(Shard shard) {
        return shard.sequenceNumberRange().endingSequenceNumber() == null;
    }
}
