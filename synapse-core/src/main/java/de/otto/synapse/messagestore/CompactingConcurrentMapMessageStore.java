package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static de.otto.synapse.messagestore.Indexers.noOpIndexer;

/**
 * Concurrent implementation of a MessageStore that is compacting entries by {@link Message#getKey() of}.
 * <p>
 *     The ordering of entries is guaranteed by using a (on-heap) ConcurrentSkipListSet for keys.
 * </p>
 * <p>
 *     The entries are stored in a ConcurrentMap like, for example, ChronicleMap. This way, the entries
 *     can be stored off-heap, so large numbers of entries can be stored in memory, without getting problems
 *     with Java garbage-collecting.
 * </p>
 */
@ThreadSafe
public class CompactingConcurrentMapMessageStore implements MessageStore {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentSkipListSet<String> compactedAndOrderedKeys = new ConcurrentSkipListSet<>();
    private final ConcurrentMap<String, MessageStoreEntry> entries;
    private final ConcurrentMap<String,ConcurrentSkipListSet<String>> indexes = new ConcurrentHashMap<>();
    private final InMemoryChannelPositions channelPositions = new InMemoryChannelPositions();
    private final boolean removeNullPayloadMessages;
    private final Indexer indexer;
    private final String name;

    public CompactingConcurrentMapMessageStore(final String name,
                                               final boolean removeNullPayloadMessages,
                                               final ConcurrentMap<String, MessageStoreEntry> messageMap) {
        this(name, removeNullPayloadMessages, messageMap, noOpIndexer());
    }

    public CompactingConcurrentMapMessageStore(final String name,
                                               final boolean removeNullPayloadMessages,
                                               final ConcurrentMap<String, MessageStoreEntry> messageMap,
                                               final Indexer indexer) {
        this.name = name;
        this.entries = messageMap;
        this.removeNullPayloadMessages = removeNullPayloadMessages;
        this.indexer = indexer;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void add(final MessageStoreEntry entry) {
        final String messageKey = entry.getChannelName() + ":" + entry.getTextMessage().getKey().compactionKey();
        lock.writeLock().lock();
        try {
            final MessageStoreEntry indexedEntry = indexer.index(entry);
            if (entry.getTextMessage().getPayload() == null && removeNullPayloadMessages) {
                entries.remove(messageKey);
                compactedAndOrderedKeys.remove(messageKey);
                indexedEntry.getFilterValues().forEach((key, value) -> {
                    final String indexKey = indexKeyOf(key, value);
                    if (indexes.containsKey(indexKey)) {
                        indexes.get(indexKey).remove(messageKey);
                    }
                });
            } else {
                entries.put(messageKey, indexedEntry);
                compactedAndOrderedKeys.add(messageKey);

                indexedEntry.getFilterValues().forEach((key, value) -> {
                    final String indexKey = indexKeyOf(key, value);
                    if (!indexes.containsKey(indexKey)) {
                        indexes.put(indexKey, new ConcurrentSkipListSet<>());
                    }
                    indexes.get(indexKey).add(messageKey);
                });

            }
            channelPositions.updateFrom(entry);
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
            return compactedAndOrderedKeys
                    .stream()
                    .map(entries::get);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<MessageStoreEntry> stream(final Index index, final String value) {
        String indexKey = indexKeyOf(index, value);
        if (indexes.containsKey(indexKey)) {
            return indexes.get(indexKey).stream().map(entries::get);
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
