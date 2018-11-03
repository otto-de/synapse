package de.otto.synapse.endpoint.receiver.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.concurrent.atomic.AtomicReference;

import static de.otto.synapse.endpoint.receiver.kinesis.KinesisStreamInfo.builder;
import static java.lang.String.format;

public class KinesisStreamInfoProvider {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamInfoProvider.class);

    private final KinesisAsyncClient kinesisAsyncClient;

    public KinesisStreamInfoProvider(final KinesisAsyncClient kinesisAsyncClient) {
        this.kinesisAsyncClient = kinesisAsyncClient;
    }

    /**
     * Returns stream information for the given Kinesis stream.
     *
     * @param channelName the name of the stream
     * @return KinesisStreamInfo
     * @throws IllegalArgumentException if the stream does not exist
     */
    public KinesisStreamInfo getStreamInfo(final String channelName) {

        try {

            final DescribeStreamRequest request = DescribeStreamRequest.builder().streamName(channelName).build();

            DescribeStreamResponse response = kinesisAsyncClient.describeStream(request).join();

            final KinesisStreamInfo.Builder streamInfoBuilder = builder()
                    .withChannelName(channelName)
                    .withArn(response.streamDescription().streamARN());

            String lastSeenShardId = addShardInfoFromResponse(response, streamInfoBuilder);

            while (response.streamDescription().hasMoreShards()) {
                response = kinesisAsyncClient.describeStream(DescribeStreamRequest.builder()
                        .streamName(channelName)
                        .exclusiveStartShardId(lastSeenShardId)
                        .build())
                        .join();
                lastSeenShardId = addShardInfoFromResponse(response, streamInfoBuilder);
            }

            return streamInfoBuilder.build();
        } catch (final ResourceNotFoundException e) {
            throw new IllegalArgumentException(format("Kinesis channel %s does not exist: %s", channelName, e.getMessage()));
        }
    }

    private String addShardInfoFromResponse(DescribeStreamResponse response, KinesisStreamInfo.Builder streamInfoBuilder) {
        AtomicReference<String> lastShardId = new AtomicReference<>(null);
        response
                .streamDescription()
                .shards()
                .stream()
                .forEach(shard -> {
                    lastShardId.set(shard.shardId());
                    streamInfoBuilder.withShard(shard.shardId(), isShardOpen(shard));
                });
        return lastShardId.get();
    }

    private boolean isShardOpen(Shard shard) {
        if (shard.sequenceNumberRange().endingSequenceNumber() == null) {
            return true;
        } else {
            LOG.warn("Shard with id {} is closed. Cannot retrieve data.", shard.shardId());
            return false;
        }
    }
}
