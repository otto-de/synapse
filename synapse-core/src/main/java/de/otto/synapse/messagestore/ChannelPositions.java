package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableSet;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import net.jcip.annotations.ThreadSafe;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static de.otto.synapse.channel.ChannelPosition.*;

@ThreadSafe
class ChannelPositions {

    private final ConcurrentMap<String, ChannelPosition> channelPositions = new ConcurrentHashMap<>();

    void updateFrom(final MessageStoreEntry entry) {
        channelPositions.compute(entry.getChannelName(), (key, existing) -> {
            final Optional<ShardPosition> shardPosition = entry
                    .getTextMessage()
                    .getHeader()
                    .getShardPosition();
            if (existing != null) {
                return shardPosition.map(s -> merge(existing, channelPosition(s))).orElse(existing);
            } else {
                return shardPosition.map(ChannelPosition::channelPosition).orElseGet(ChannelPosition::fromHorizon);
            }
        });
    }

    public ImmutableSet<String> getChannelNames() {
        return ImmutableSet.copyOf(channelPositions.keySet());
    }

    public ChannelPosition getLatestChannelPosition(final String channelName) {
        return channelPositions.getOrDefault(channelName, fromHorizon());
    }
}
