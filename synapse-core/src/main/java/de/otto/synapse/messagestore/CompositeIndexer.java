package de.otto.synapse.messagestore;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * An {@code Indexer} that is a composition of other Indexers.
 */
@Beta
public class CompositeIndexer implements Indexer {

    private final ImmutableList<Indexer> indexers;
    private final ImmutableSet<Index> indexes;

    /**
     * Creates a {@code CompositeIndex} from a list of {@code Indexers}.
     *
     * <p>If there are multiple {@code Indexers} responsible for the same {@link Index},
     * the first {@code Indexer} instance will be used and all other instances will not be used
     * to calculate the {@code Index}.</p>
     *
     * @param indexers the indexers used to build the composite
     */
    public CompositeIndexer(final ImmutableList<Indexer> indexers) {
        if (indexers.size() < 2) {
            throw new IllegalArgumentException("Can not build a CompositeIndexer from less than two elements");
        }
        this.indexes = indexers
                .stream()
                .flatMap((Indexer indexer) -> indexer.getIndexes()
                        .stream())
                .collect(toImmutableSet());
        this.indexers = indexers;
    }

    @Nonnull
    @Override
    public ImmutableSet<Index> getIndexes() {
        return indexes;
    }

    @Override
    public boolean supports(@Nonnull final Index index) {
        return indexes.contains(index);
    }

    @Override
    public String calc(@Nonnull final Index index, @Nonnull final MessageStoreEntry entry) {
        if (supports(index)) {
            return indexers
                    .stream()
                    .filter(indexer -> indexer.supports(index))
                    .findFirst()
                    .map(indexer -> indexer.calc(index, entry))
                    .orElseThrow(IllegalStateException::new);
        } else {
            throw new IllegalArgumentException("Unknown index " + index.getName());
        }
    }

    @Nonnull
    @Override
    public MessageStoreEntry index(@Nonnull MessageStoreEntry entry) {
        final ImmutableMap.Builder<Index,String> filterValues = ImmutableMap.builder();
        getIndexes().forEach(index -> filterValues.put(index, calc(index, entry)));
        return MessageStoreEntry.of(entry.getChannelName(), filterValues.build(), entry.getTextMessage());
    }
}
