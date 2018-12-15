package de.otto.synapse.channel;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.message.Message;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

public final class ShardResponse {
    private final String channelName;
    private final Duration durationBehind;
    private final Duration runtime;
    private final ShardPosition shardPosition;
    private final List<Message<String>> messages;

    public ShardResponse(final String channelName,
                         final ImmutableList<Message<String>> messages,
                         final ShardPosition shardPosition,
                         final Duration runtime,
                         final Duration durationBehind) {

        this.channelName = channelName;
        this.runtime = runtime;
        this.shardPosition = shardPosition;
        this.messages = messages;
        this.durationBehind = durationBehind;
    }

    public String getChannelName() {
        return channelName;
    }

    public String getShardName() {
        return shardPosition.shardName();
    }

    public ShardPosition getShardPosition() {
        return shardPosition;
    }

    public Duration getDurationBehind() {
        return durationBehind;
    }

    public Duration getRuntime() {
        return runtime;
    }

    public List<Message<String>> getMessages() {
        return messages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardResponse that = (ShardResponse) o;
        return Objects.equals(channelName, that.channelName) &&
                Objects.equals(durationBehind, that.durationBehind) &&
                Objects.equals(runtime, that.runtime) &&
                Objects.equals(shardPosition, that.shardPosition) &&
                Objects.equals(messages, that.messages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelName, durationBehind, runtime, shardPosition, messages);
    }

    @Override
    public String toString() {
        return "ShardResponse{" +
                "channelName='" + channelName + '\'' +
                ", durationBehind=" + durationBehind +
                ", runtime=" + runtime +
                ", shardPosition=" + shardPosition +
                ", messages=" + messages +
                '}';
    }

}
