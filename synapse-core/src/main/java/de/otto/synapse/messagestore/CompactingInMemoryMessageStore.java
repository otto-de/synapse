package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Concurrent in-memory implementation of a MessageStore that is compacting entries by {@link Message#getKey() of}.
 * <p>
 *     Messages are stored using a ConcurrentNavigableMap.
 * </p>
 */
@ThreadSafe
public class CompactingInMemoryMessageStore implements WritableMessageStore {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentNavigableMap<String, MessageStoreEntry> entries = new ConcurrentSkipListMap<>();
    private final InMemoryChannelPositions channelPositions = new InMemoryChannelPositions();
    private final boolean removeNullPayloadMessages;
    private final String name;

    public CompactingInMemoryMessageStore(final String name) {
        this.name = name;
        this.removeNullPayloadMessages = true;
    }

    public CompactingInMemoryMessageStore(final String name, final boolean removeNullPayloadMessages) {
        this.removeNullPayloadMessages = removeNullPayloadMessages;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void add(final MessageStoreEntry entry) {
        lock.writeLock().lock();
        try {
            final String internalKey = entry.getChannelName() + ":" + entry.getTextMessage().getKey().compactionKey();
            if (entry.getTextMessage().getPayload() == null && removeNullPayloadMessages) {
                entries.remove(internalKey);
            } else {
                entries.put(internalKey, entry);
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
    public Stream<MessageStoreEntry> streamAll() {
        lock.readLock().lock();
        try {
            return entries.values().stream();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        return entries.size();
    }
}
