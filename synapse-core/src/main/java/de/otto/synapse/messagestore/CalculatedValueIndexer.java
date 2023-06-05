package de.otto.synapse.messagestore;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nonnull;

import java.util.function.Function;

import static de.otto.synapse.messagestore.MessageStoreEntry.of;
import static java.util.Objects.requireNonNull;

/**
 * An {@code Indexer} that is using a {@code Function} to calculate the value for a single {@link Index}.
 */
@Beta
public class CalculatedValueIndexer implements Indexer {
    private final Index index;
    private final Function<MessageStoreEntry, String> calculator;

    /**
     * Creates a {@code CalculatedValueIndexer}.
     *
     * @param index the {@code Index} that is calculated by the {@code calculator} function
     * @param calculator the function that is used to calculate the value for the index
     */
    public CalculatedValueIndexer(final @Nonnull Index index,
                           final @Nonnull Function<MessageStoreEntry, String> calculator) {
        this.index = requireNonNull(index);
        this.calculator = calculator;
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
    public String calc(final @Nonnull Index index,
                       final @Nonnull MessageStoreEntry entry) {
        if (supports(index)) {
            return calculator.apply(entry);
        } else {
            throw new IllegalArgumentException("Unknown index " + index.getName());
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
