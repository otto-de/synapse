package de.otto.synapse.channel;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.*;

import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.ImmutableMap.of;
import static java.util.Arrays.asList;

public final class ChannelPosition implements Serializable {
    private final ImmutableMap<String, String> shardPositions;

    public static ChannelPosition fromHorizon() {
        return channelPosition(ImmutableMap.of());
    }

    public static ChannelPosition merge(final ChannelPosition... channelPositions) {
        if (channelPositions.length == 0) {
            throw new IllegalArgumentException("Parameter channelPositions must contain at least one element");
        }
        return merge(asList(channelPositions));
    }

    public static ChannelPosition merge(final List<ChannelPosition> channelPositions) {
        if (channelPositions.isEmpty()) {
            throw new IllegalArgumentException("Parameter channelPositions must contain at least one element");
        }
        final Map<String,String> shardPositions = new LinkedHashMap<>();
        channelPositions.forEach(streamPosition -> streamPosition
                .shards()
                .forEach(shardId ->
                        shardPositions.put(shardId, streamPosition.positionOf(shardId)))
        );
        return new ChannelPosition(copyOf(shardPositions));
    }

    public static ChannelPosition shardPosition(final String shardId, final String position) {
        return new ChannelPosition(of(shardId, position));
    }

    public static ChannelPosition channelPosition(final ImmutableMap<String, String> shardPositions) {
        return new ChannelPosition(shardPositions);
    }

    protected ChannelPosition(final ImmutableMap<String, String> shardPositions) {
        this.shardPositions = shardPositions;
    }

    public Set<String> shards() {
        return shardPositions.keySet();
    }

    /**
     * Returns the position of a single shard, or "0", if there is no information about the shard.
     *
     * @param shard the shard id
     * @return position or "0"
     */
    public String positionOf(final String shard) {
        // TODO: "0" used as magic value for "from horizon"
        return shardPositions.getOrDefault(shard, "0");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChannelPosition that = (ChannelPosition) o;
        return Objects.equals(shardPositions, that.shardPositions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardPositions);
    }

    @Override
    public String toString() {
        return "StreamPosition{" +
                "shardPositions=" + shardPositions +
                '}';
    }
}
