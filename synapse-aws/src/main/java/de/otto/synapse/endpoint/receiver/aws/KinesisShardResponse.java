package de.otto.synapse.endpoint.receiver.aws;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.Message;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import static de.otto.synapse.message.aws.KinesisMessage.kinesisMessage;
import static java.time.Duration.ofMillis;
import static java.util.stream.Collectors.toList;

public class KinesisShardResponse {
    private final String channelName;
    private final String shardName;
    private final Duration durationBehind;
    private final long runtime;
    private final ShardPosition shardPosition;
    private final List<Message<String>> messages;

    public KinesisShardResponse(final String channelName,
                                final String shardName,
                                final ShardPosition shardPosition,
                                final GetRecordsResponse recordsResponse,
                                final long runtime) {
        this.channelName = channelName;
        this.shardName = shardName;
        this.shardPosition = shardPosition;
        this.runtime = runtime;
        this.durationBehind = ofMillis(recordsResponse.millisBehindLatest());
        this.messages = recordsResponse.records()
                .stream()
                .map(record -> kinesisMessage(shardName, record))
                .collect(toList());
    }

    public String getChannelName() {
        return channelName;
    }

    public String getShardName() {
        return shardName;
    }

    public ShardPosition getShardPosition() {
        return shardPosition;
    }

    public Duration getDurationBehind() {
        return durationBehind;
    }

    public long getRuntime() {
        return runtime;
    }

    public List<Message<String>> getMessages() {
        return messages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KinesisShardResponse response = (KinesisShardResponse) o;
        return runtime == response.runtime &&
                Objects.equals(channelName, response.channelName) &&
                Objects.equals(shardName, response.shardName) &&
                Objects.equals(durationBehind, response.durationBehind) &&
                Objects.equals(shardPosition, response.shardPosition) &&
                Objects.equals(messages, response.messages);
    }

    @Override
    public int hashCode() {

        return Objects.hash(channelName, shardName, durationBehind, runtime, shardPosition, messages);
    }

    @Override
    public String toString() {
        return "KinesisShardResponse{" +
                "channelName='" + channelName + '\'' +
                ", shardName='" + shardName + '\'' +
                ", durationBehind=" + durationBehind +
                ", runtime=" + runtime +
                ", shardPosition=" + shardPosition +
                ", messages=" + messages +
                '}';
    }
}
