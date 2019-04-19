package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableSet;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Key;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Thread-safe in-memory (on heap) implementation of a MessageStore that is compacting entries by the message's
 * {@link Key#compactionKey()}.
 *
 * <p><em>Features:</em></p>
 * <ul>
 *     <li>Thread-Safe</li>
 *     <li>Supports a maximum capacity, with eviction of least-recently used (LRU) messages </li>
 *     <li>Supports instant compaction of messages.</li>
 *     <li>No indexing of messages is supported.</li>
 * </ul>
 */
@ThreadSafe
public class OnHeapCompactingMessageStore implements MessageStore {


    // TODO: Introduce Builder
    // TODO: Make capacity configurable
    // TODO: Support off-heap maps

    private final long maxCapacity;
    private final boolean removeNullPayloadMessages;
    private final ChannelPositions channelPositions = new ChannelPositions();
    private final ConcurrentMap<Long, MessageStoreEntry> entries;
    private final ConcurrentMap<String, Long> internalKeyToIndexMapping = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong nextKey = new AtomicLong();

    public OnHeapCompactingMessageStore(final boolean removeNullPayloadMessages) {
        this(removeNullPayloadMessages, Long.MAX_VALUE);
    }

    public OnHeapCompactingMessageStore(final boolean removeNullPayloadMessages,
                                        final long maxCapacity) {
        this.removeNullPayloadMessages = removeNullPayloadMessages;
        this.maxCapacity = maxCapacity;
        this.entries = new ConcurrentLinkedHashMap.Builder<Long,MessageStoreEntry>()
                .initialCapacity(1000)
                .maximumWeightedCapacity(maxCapacity)
                .build();
    }

    @Override
    public void add(final MessageStoreEntry entry) {
        lock.writeLock().lock();
        try {
            final String internalKey = entry.getChannelName() + ":" + entry.getTextMessage().getKey().compactionKey();
            final long index = nextKey.getAndIncrement();

            if (entry.getTextMessage().getPayload() == null && removeNullPayloadMessages) {
                final Long previousIndex = internalKeyToIndexMapping.get(internalKey);
                if (previousIndex != null) {
                    entries.remove(previousIndex);
                }
                internalKeyToIndexMapping.remove(internalKey);
            } else {
                final Long previousIndex = internalKeyToIndexMapping.get(internalKey);
                if (previousIndex != null) {
                    entries.put(previousIndex, entry);
                } else {
                    entries.put(index, entry);
                    internalKeyToIndexMapping.put(internalKey, index);
                }
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
            return channelPositions.getChannelNames();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ImmutableSet<Index> getIndexes() {
        return ImmutableSet.of();
    }

    @Override
    public ChannelPosition getLatestChannelPosition(final String channelName) {
        lock.readLock().lock();
        try {
            return channelPositions.getLatestChannelPosition(channelName);
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
    public Stream<MessageStoreEntry> stream(Index index, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        return entries.size();
    }

}
