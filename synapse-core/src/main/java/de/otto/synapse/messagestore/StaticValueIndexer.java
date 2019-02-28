package de.otto.synapse.messagestore;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;

import static de.otto.synapse.messagestore.MessageStoreEntry.of;
import static java.util.Objects.requireNonNull;

/**
 * An {@code Indexer} that is creating a single {@link Index} by setting the indexed value to some static text.
 */
@Beta
public class StaticValueIndexer implements Indexer {
    private final Index index;
    private final String value;

    /**
     * Creates a {@code StaticValueIndexer}.
     *
     * @param index the {@code Index}
     * @param value the static value that will be used to set the index
     */
    public StaticValueIndexer(final Index index, final String value) {
        this.index = requireNonNull(index);
        this.value = requireNonNull(value);
    }

    @Nonnull
    @Override
    public ImmutableSet<Index> getIndexes() {
        return ImmutableSet.of(this.index);
    }

    @Override
    public boolean supports(final @Nonnull Index index) {
        return this.index.equals(index);
    }

    @Override
    public String calc(final @Nonnull Index index, final @Nonnull MessageStoreEntry entry) {
        if (supports(index)) {
            return value;
        } else {
            throw new IllegalArgumentException("Unknown index " + index.name());
        }
    }

    @Nonnull
    @Override
    public MessageStoreEntry index(@Nonnull MessageStoreEntry entry) {
        return of(
                entry.getChannelName(),
                ImmutableMap.of(index, calc(index, entry)),
                entry.getTextMessage());
    }
}
