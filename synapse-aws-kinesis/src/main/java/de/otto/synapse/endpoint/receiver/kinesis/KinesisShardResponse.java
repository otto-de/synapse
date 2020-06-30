package de.otto.synapse.endpoint.receiver.kinesis;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.ShardResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static de.otto.synapse.channel.ShardResponse.shardResponse;
import static java.time.Duration.ofMillis;

public class KinesisShardResponse {

    public static ShardResponse kinesisShardResponse(final ShardPosition shardPosition,
                                                     final GetRecordsResponse recordsResponse) {
        final KinesisDecoder kinesisDecoder = new KinesisDecoder();
        return shardResponse(
                shardPosition,
                ofMillis(recordsResponse.millisBehindLatest() == null ? Long.MAX_VALUE : recordsResponse.millisBehindLatest()),
                recordsResponse.records()
                        .stream()
                        .map(record -> kinesisDecoder.apply(new RecordWithShard(shardPosition.shardName(), record)))
                        .collect(toImmutableList())
        );
    }

}
