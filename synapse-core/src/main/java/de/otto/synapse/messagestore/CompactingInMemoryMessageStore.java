package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static de.otto.synapse.messagestore.Indexers.noOpIndexer;

/**
 * Concurrent in-memory implementation of a MessageStore that is compacting entries by {@link Message#getKey() key}.
 * <p>
 *     Messages are stored using a ConcurrentNavigableMap.
 * </p>
 */
@ThreadSafe
public class CompactingInMemoryMessageStore implements MessageStore {

    private final String name;
    private final boolean removeNullPayloadMessages;
    private final Indexer indexer;
    private final InMemoryChannelPositions channelPositions = new InMemoryChannelPositions();
    private final ConcurrentNavigableMap<String, MessageStoreEntry> entries = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<String, ConcurrentNavigableMap<String, MessageStoreEntry>> indexes = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public CompactingInMemoryMessageStore(final String name,
                                          final boolean removeNullPayloadMessages) {
        this.removeNullPayloadMessages = removeNullPayloadMessages;
        this.name = name;
        this.indexer = noOpIndexer();
    }

    public CompactingInMemoryMessageStore(final String name,
                                          final boolean removeNullPayloadMessages,
                                          final Indexer indexer) {
        this.removeNullPayloadMessages = removeNullPayloadMessages;
        this.name = name;
        this.indexer = indexer;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void add(final MessageStoreEntry entry) {
        lock.writeLock().lock();
        try {
            final MessageStoreEntry indexedEntry = indexer.index(entry);
            final String internalKey = entry.getChannelName() + ":" + entry.getTextMessage().getKey().compactionKey();
            if (entry.getTextMessage().getPayload() == null && removeNullPayloadMessages) {
                entries.remove(internalKey);

                indexedEntry.getFilterValues().forEach((key, value) -> {
                    final String indexKey = indexKeyOf(key, value);
                    if (indexes.containsKey(indexKey)) {
                        indexes.get(indexKey).remove(internalKey);
                    }
                });
            } else {
                entries.put(internalKey, indexedEntry);

                indexedEntry.getFilterValues().forEach((key, value) -> {
                    final String indexKey = indexKeyOf(key, value);
                    if (!indexes.containsKey(indexKey)) {
                        indexes.put(indexKey, new ConcurrentSkipListMap<>());
                    }
                    indexes.get(indexKey).put(internalKey, indexedEntry);
                });
            }
            channelPositions.updateFrom(indexedEntry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Set<String> getChannelNames() {
        lock.readLock().lock();
        try {
            return channelPositions.channelNames();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ChannelPosition getLatestChannelPosition(final String channelName) {
        lock.readLock().lock();
        try {
            return channelPositions.positionOf(channelName);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<MessageStoreEntry> stream() {
        lock.readLock().lock();
        try {
            return entries.values().stream();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<MessageStoreEntry> stream(final Index index, final String value) {
        String indexKey = indexKeyOf(index, value);
        if (indexes.containsKey(indexKey)) {
            return indexes.get(indexKey).values().stream();
        } else {
            return Stream.empty();
        }
    }

    @Override
    public int size() {
        return entries.size();
    }

    private String indexKeyOf(Index index, String value) {
        return index.name() + "#" + value;
    }
}
