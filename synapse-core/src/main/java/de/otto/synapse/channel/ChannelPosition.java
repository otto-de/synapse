package de.otto.synapse.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.*;

import static java.util.Arrays.asList;

public final class ChannelPosition implements Serializable {
    private final ImmutableMap<String, ShardPosition> shardPositions;

    public static ChannelPosition fromHorizon() {
        return channelPosition(ImmutableList.of());
    }

    public static ChannelPosition merge(final ChannelPosition... channelPositions) {
        if (channelPositions.length == 0) {
            return fromHorizon();
        } else {
            return merge(asList(channelPositions));
        }
    }

    public static ChannelPosition merge(final ChannelPosition channelPosition,
                                        final ShardPosition shardPosition) {
        return merge(channelPosition, channelPosition(shardPosition));
    }

    public static ChannelPosition merge(final List<ChannelPosition> channelPositions) {
        if (channelPositions.isEmpty()) {
            throw new IllegalArgumentException("Parameter channelPositions must contain at least one element");
        }
        final Map<String,ShardPosition> shardPositions = new LinkedHashMap<>();
        channelPositions.forEach(streamPosition -> streamPosition
                .shards()
                .forEach(shardId ->
                        shardPositions.put(shardId, streamPosition.shard(shardId)))
        );
        return new ChannelPosition(ImmutableList.copyOf(shardPositions.values()));
    }

    public static ChannelPosition channelPosition(final ShardPosition... shardPositions) {
        if (shardPositions.length == 0) {
            return fromHorizon();
        } else {
            return new ChannelPosition(asList(shardPositions));
        }
    }

    public static ChannelPosition channelPosition(final Iterable<ShardPosition> shardPositions) {
        return new ChannelPosition(shardPositions);
    }

    protected ChannelPosition(final Iterable<ShardPosition> shardPositions) {
        this.shardPositions = Maps.uniqueIndex(shardPositions, ShardPosition::shardName);
    }

    /**
     * Returns a Set containing all shard names of this ChannelPosition.
     *
     * @return set of shard names
     */
    public Set<String> shards() {
        return shardPositions.keySet();
    }

    /**
     * Returns the position of a single shard, or {@link ShardPosition#fromHorizon(String)}, if there is no information
     * about the shard.
     *
     * @param shard the shard id
     * @return ShardPosition
     */
    @Nonnull
    public ShardPosition shard(final String shard) {
        return shardPositions.getOrDefault(shard, ShardPosition.fromHorizon(shard));
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
