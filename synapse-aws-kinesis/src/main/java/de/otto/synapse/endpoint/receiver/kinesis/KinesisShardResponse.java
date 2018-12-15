package de.otto.synapse.endpoint.receiver.kinesis;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.ShardResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static de.otto.synapse.message.kinesis.KinesisMessage.kinesisMessage;
import static java.time.Duration.ofMillis;

public class KinesisShardResponse {

    public static ShardResponse kinesisShardResponse(final String channelName,
                                                            final ShardPosition shardPosition,
                                                            final GetRecordsResponse recordsResponse,
                                                            final long runtime) {
        return new ShardResponse(channelName, recordsResponse.records()
                                .stream()
                                .map(record -> kinesisMessage(shardPosition.shardName(), record))
                                .collect(toImmutableList()), shardPosition, ofMillis(runtime), ofMillis(recordsResponse.millisBehindLatest()));
    }

}
