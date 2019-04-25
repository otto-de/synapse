package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.TextMessage;
import org.dizitart.no2.Cursor;
import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteCollection;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.builder;
import static org.dizitart.no2.Document.createDocument;
import static org.dizitart.no2.IndexOptions.indexOptions;
import static org.dizitart.no2.IndexType.NonUnique;
import static org.dizitart.no2.filters.Filters.eq;

/**
 * A {@code MessageStore} that is storing messages off the heap and supports indexing of messages.
 *
 * <p>Implementation Hints:</p>
 * <ul>
 *     <li>The store is using Nitrite to messages</li>
 * </ul>
 *
 * @see <a href="https://www.dizitart.org/nitrite-database.html">Nitrite Database</a>
 */
public class OffHeapIndexingMessageStore implements MessageStore {

    private final Nitrite nitrite;
    private final NitriteCollection messages;
    private final ChannelPositions channelPositions = new ChannelPositions();
    private final Indexer indexer;

    public OffHeapIndexingMessageStore(final String name) {
        this(name, Indexers.noOpIndexer());
    }
    
    public OffHeapIndexingMessageStore(final String name, final Indexer indexer) {
        nitrite = Nitrite
                .builder()
                .openOrCreate();
        messages = nitrite.getCollection(name + "-messages");
        indexer.getIndexes().forEach(index -> {
            messages.createIndex("_idx_" + index.getName(), indexOptions(NonUnique));
        });
        this.indexer = indexer;
    }

    @Override
    public Set<String> getChannelNames() {
        return channelPositions.getChannelNames();
    }

    @Override
    public ImmutableSet<Index> getIndexes() {
        return indexer.getIndexes();
    }

    @Override
    public ChannelPosition getLatestChannelPosition(String channelName) {
        return channelPositions.getLatestChannelPosition(channelName);
    }

    @Override
    public Stream<MessageStoreEntry> stream() {
        return toEntryStream(messages.find());
    }

    @Override
    public Stream<MessageStoreEntry> stream(final Index index, final String value) {
        return toEntryStream(messages.find(eq("_idx_" + index.getName(), value)));
    }

    @Override
    public void add(final @Nonnull MessageStoreEntry entry) {
        final MessageStoreEntry indexedEntry = indexer.index(entry);
        final Document document = createDocument("channelName", indexedEntry.getChannelName());
        indexedEntry.getFilterValues().forEach((index, value) -> {
            document.put("_idx_" + index.getName(), value);
        });
        document.put("message", entry.getTextMessage());
        messages.insert(document);
        channelPositions.updateFrom(entry);
    }

    @Override
    public long size() {
        return messages.size();
    }

    @Override
    public void close() {
        nitrite.close();
    }

    @Nonnull
    private Stream<MessageStoreEntry> toEntryStream(Cursor cursor) {
        return Streams.stream(cursor).map(document ->
                {
                    final ImmutableMap.Builder<Index, String> filterValues = builder();
                    indexer.getIndexes().forEach(index -> {
                        String value = document.get("_idx_" + index.getName(), String.class);
                        if (value != null) {
                            filterValues.put(index, value);
                        }
                    });
                    return MessageStoreEntry.of(
                            document.get("channelName", String.class),
                            filterValues.build(),
                            document.get("message", TextMessage.class));
                }
        );
    }
}
