package de.otto.synapse.channel;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.message.Message;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class ChannelResponse {

    private final String channelName;
    private final ImmutableList<ShardResponse> shardResponses;

    public static ChannelResponse channelResponse(final String channelName,
                                                  final ImmutableList<ShardResponse> shardResponses) {
        return new ChannelResponse(channelName, shardResponses);
    }

    public static ChannelResponse channelResponse(final String channelName,
                                                  final ShardResponse... shardResponses) {
        return new ChannelResponse(
                channelName,
                shardResponses != null
                        ? ImmutableList.copyOf(shardResponses)
                        : ImmutableList.of());
    }

    private ChannelResponse(final String channelName,
                            final ImmutableList<ShardResponse> shardResponses) {
        if (shardResponses.isEmpty()) {
            throw new IllegalArgumentException("Unable to create ChannelResponse without KinesisShardResponses");
        }
        this.channelName = channelName;
        this.shardResponses = shardResponses;
    }

    public String getChannelName() {
        return channelName;
    }

    public ChannelDurationBehind getChannelDurationBehind() {
        final ChannelDurationBehind.Builder durationBehind = channelDurationBehind();
        shardResponses.forEach((response) -> durationBehind.with(response.getShardName(), response.getDurationBehind()));
        return durationBehind.build();
    }

    public List<Message<String>> getMessages() {
        return shardResponses
                .stream()
                .flatMap(response -> response.getMessages().stream())
                .collect(toList());
    }

    public Set<String> getShardNames() {
        return shardResponses
                .stream()
                .map(ShardResponse::getShardName)
                .collect(toSet());
    }

    public ImmutableList<ShardResponse> getShardResponses() {
        return shardResponses;
    }

    public ChannelPosition getChannelPosition() {
        return channelPosition(shardResponses
                .stream()
                .map(ShardResponse::getShardPosition)
                .collect(Collectors.toList()));
    }
}
