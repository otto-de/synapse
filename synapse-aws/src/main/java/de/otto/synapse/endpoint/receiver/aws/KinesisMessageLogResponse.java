package de.otto.synapse.endpoint.receiver.aws;

import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.message.Message;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class KinesisMessageLogResponse {

    private final Map<String, KinesisShardResponse> shardResponses;

    public KinesisMessageLogResponse(final List<KinesisShardResponse> shardResponses) {
        this.shardResponses = uniqueIndex(shardResponses, KinesisShardResponse::getShardName);
    }

    public String getChannelName() {
        return shardResponses.values().iterator().next().getChannelName();
    }

    public ChannelDurationBehind getChannelDurationBehind() {
        final ChannelDurationBehind.Builder durationBehind = ChannelDurationBehind.channelDurationBehind();
        shardResponses.forEach((key, value) -> durationBehind.with(key, value.getDurationBehind()));
        return durationBehind.build();
    }

    public List<Message<String>> getMessages() {
        return shardResponses
                .values()
                .stream()
                .flatMap(response -> response.getMessages().stream())
                .collect(toList());
    }

    public Set<String> getShardNames() {
        return shardResponses
                .values()
                .stream()
                .map(KinesisShardResponse::getShardName)
                .collect(toSet());
    }

    public Collection<KinesisShardResponse> getShardResponses() {
        return shardResponses.values();
    }
}
