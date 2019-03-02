package de.otto.synapse.messagestore;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;

import javax.annotation.Nonnull;

import static de.otto.synapse.messagestore.Index.*;
import static java.util.Objects.requireNonNull;

/**
 * Utility class used to create {@link Indexer} instances.
 */
@Beta
public class Indexers {

    private Indexers() {}

    /**
     * @return no-op Indexer that is not doing anything.
     */
    public static Indexer noOpIndexer() {
        return new Indexer() {
            @Nonnull
            @Override
            public ImmutableSet<Index> getIndexes() {
                return ImmutableSet.of();
            }

            @Override
            public boolean supports(@Nonnull Index index) {
                return false;
            }

            @Nonnull
            @Override
            public String calc(@Nonnull Index index, @Nonnull MessageStoreEntry entry) {
                return "";
            }

            @Nonnull
            @Override
            public MessageStoreEntry index(@Nonnull MessageStoreEntry entry) {
                return entry;
            }
        };
    }

    /**
     * Returns a composite Indexer that is able to support several indexes.
     *
     * @param indexers the list of Indexers
     * @return composite Indexer
     */
    public static Indexer composite(final ImmutableList<Indexer> indexers) {
        return new CompositeIndexer(indexers);
    }

    /**
     * Returns a composite Indexer that is able to support several indexes.
     *
     * @param indexers the list of Indexers
     * @return composite Indexer
     */
    public static Indexer composite(final Indexer... indexers) {
        requireNonNull(indexers, "Parameter must not be null");
        return new CompositeIndexer(ImmutableList.copyOf(indexers));
    }

    /**
     * Returns an Indexer that is indexing all entries with static value that is identifying the origin of some message.
     *
     * @return origin indexer
     */
    public static Indexer originIndexer(final @Nonnull String origin) {
        return new StaticValueIndexer(ORIGIN, origin);
    }

    /**
     * Returns an Indexer that is indexing all entries with static value that is identifying the service-instance of
     * some message.
     *
     * <p>In most cases, a {@code hostname:port} or {@code service-name@hostname:port} value is sufficient to
     * identify the instance of some service</p>
     * @return serviceInstance indexer
     */
    public static Indexer serviceInstanceIndexer(final @Nonnull String serviceInstance) {
        return new StaticValueIndexer(SERVICE_INSTANCE, serviceInstance);
    }

    /**
     * Returns an Indexer that is indexing {@link TextMessage} by {@link Key#partitionKey()}.
     *
     * @return partition-key indexer
     */
    public static Indexer partitionKeyIndexer() {
        return new CalculatedValueIndexer(PARTITION_KEY, entry -> entry.getTextMessage().getKey().partitionKey());
    }

    /**
     * Returns an Indexer that is indexing {@link TextMessage} by {@link Key#partitionKey()}.
     *
     * @return partition-key indexer
     */
    public static Indexer channelNameIndexer() {
        return new CalculatedValueIndexer(CHANNEL_NAME, entry->entry.getChannelName());
    }

}
