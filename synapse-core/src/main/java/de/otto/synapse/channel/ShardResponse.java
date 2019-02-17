package de.otto.synapse.channel;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.message.TextMessage;

import java.time.Duration;
import java.util.Objects;

public final class ShardResponse {
    private final Duration durationBehind;
    private final ShardPosition shardPosition;
    private final ImmutableList<TextMessage> messages;

    private ShardResponse(final ImmutableList<TextMessage> messages,
                          final ShardPosition shardPosition,
                          final Duration durationBehind) {

        this.shardPosition = shardPosition;
        this.messages = messages;
        this.durationBehind = durationBehind;
    }

    public static ShardResponse shardResponse(final ShardPosition shardPosition,
                                              final Duration durationBehind,
                                              final ImmutableList<TextMessage> messages) {
        return new ShardResponse(messages, shardPosition, durationBehind);
    }

    public static ShardResponse shardResponse(final ShardPosition shardPosition,
                                              final Duration durationBehind,
                                              final TextMessage... messages) {
        return new ShardResponse(messages != null
                ? ImmutableList.copyOf(messages)
                : ImmutableList.of(), shardPosition, durationBehind);
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

    public ImmutableList<TextMessage> getMessages() {
        return messages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardResponse that = (ShardResponse) o;
        return Objects.equals(durationBehind, that.durationBehind) &&
                Objects.equals(shardPosition, that.shardPosition) &&
                Objects.equals(messages, that.messages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(durationBehind, shardPosition, messages);
    }

    @Override
    public String toString() {
        return "ShardResponse{" +
                "durationBehind=" + durationBehind +
                ", shardPosition=" + shardPosition +
                ", messages=" + messages +
                '}';
    }

}
