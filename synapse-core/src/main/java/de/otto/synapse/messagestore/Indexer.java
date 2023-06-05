package de.otto.synapse.messagestore;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nonnull;

/**
 * Indexes {@link MessageStoreEntry message-store entries} added to a {@link MessageStore} so it can later be
 * retrieved using {@link MessageStore#stream(Index,String)}.
 */
@Beta
public interface Indexer {

    /**
     * Returns the indexes supported by this Indexer.
     *
     * @return Set of Index instances
     */
    @Nonnull
    ImmutableSet<Index> getIndexes();

    /**
     *
     * @param index the Index
     * @return true, if the Index is supported, false otherwise.
     */
    boolean supports(@Nonnull Index index);

    /**
     * Calculates the Index value from the message-store entry.
     *
     * @param index the Index
     * @param entry the MessageStoreEntry
     * @return indexed value
     */
    @Nonnull
    String calc(@Nonnull Index index, @Nonnull MessageStoreEntry entry);

    @Nonnull
    MessageStoreEntry index(final @Nonnull MessageStoreEntry entry);
}
