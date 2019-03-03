package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableSet;
import de.otto.synapse.channel.ChannelPosition;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static de.otto.synapse.messagestore.Indexers.noOpIndexer;

/**
 * Concurrent in-memory implementation of a MessageStore that is storing all messages in insertion order.
 */
@ThreadSafe
public class InMemoryMessageStore implements MessageStore {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Deque<MessageStoreEntry> entries = new ConcurrentLinkedDeque<>();
    private final ConcurrentMap<String, Deque<MessageStoreEntry>> indexes = new ConcurrentHashMap<>();
    private final InMemoryChannelPositions channelPositions = new InMemoryChannelPositions();
    private final String name;
    private final Indexer indexer;

    public InMemoryMessageStore(final String name) {
        this.name = name;
        this.indexer = noOpIndexer();
    }

    public InMemoryMessageStore(final String name, final Indexer indexer) {
        this.name = name;
        this.indexer = indexer;
    }

    @Override
    public void add(final MessageStoreEntry entry) {
        lock.writeLock().lock();
        try {
            final MessageStoreEntry indexedEntry = indexer.index(entry);
            entries.add(indexedEntry);
            indexedEntry.getFilterValues().forEach((key, value) -> {
                final String indexKey = indexKeyOf(key, value);
                if (!indexes.containsKey(indexKey)) {
                    indexes.put(indexKey, new ConcurrentLinkedDeque<>());
                }
                indexes.get(indexKey).addLast(indexedEntry);
            });
            channelPositions.updateFrom(indexedEntry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String getName() {
        return name;
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
    public ImmutableSet<Index> getIndexes() {
        return indexer.getIndexes();
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
            return entries.stream();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<MessageStoreEntry> stream(Index index, String value) {
        String indexKey = indexKeyOf(index, value);
        if (indexes.containsKey(indexKey)) {
            return indexes.get(indexKey).stream();
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
