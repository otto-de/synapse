package de.otto.edison.eventsourcing.consumer;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

public class StreamPosition {
    private final Map<String, String> shardPositions;

    private StreamPosition(final Map<String, String> shardPositions) {
        this.shardPositions = shardPositions;
    }

    public static StreamPosition of() {
        return of(emptyMap());
    }

    public static StreamPosition of(final Map<String, String> shardPositions) {
        return new StreamPosition(shardPositions);
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
        return shardPositions.getOrDefault(shard, "0");
    }
}
