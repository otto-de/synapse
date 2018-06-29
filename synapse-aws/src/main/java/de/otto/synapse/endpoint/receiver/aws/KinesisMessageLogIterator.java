package de.otto.synapse.endpoint.receiver.aws;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.List;

import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public class KinesisMessageLogIterator {

    private final ImmutableMap<String, KinesisShardIterator> shardIterators;

    public KinesisMessageLogIterator(final @Nonnull List<KinesisShardIterator> shardIterators) {
        this.shardIterators = uniqueIndex(shardIterators, (iter) -> iter.getShardPosition().shardName());
    }

    @Nonnull
    public KinesisShardIterator getShardIterator(final String shardName) {
        return requireNonNull(
                shardIterators.get(shardName),
                "Unknown shard " + shardName
        );
    }
}
