package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

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
public class CompactingConcurrentMapMessageStore implements WritableMessageStore {

    private static final int DEFAULT_KEY_SIZE_BYTES = 128;
    private static final double DEFAULT_VALUE_SIZE_BYTES = 512;
    private static final long DEFAULT_ENTRY_COUNT = 1_000_00;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentSkipListSet<String> compactedAndOrderedKeys = new ConcurrentSkipListSet<>();
    private final ConcurrentMap<String, MessageStoreEntry> entries;
    private final InMemoryChannelPositions channelPositions = new InMemoryChannelPositions();
    private final boolean removeNullPayloadMessages;
    private final String name;

    public CompactingConcurrentMapMessageStore(final String name) {
        this(name, true);
    }

    public CompactingConcurrentMapMessageStore(final String name, final boolean removeNullPayloadMessages) {
        // TODO why ChronicleMap as a default??
        this(name, removeNullPayloadMessages, ChronicleMapBuilder.of(String.class, MessageStoreEntry.class)
                .averageKeySize(DEFAULT_KEY_SIZE_BYTES)
                .averageValueSize(DEFAULT_VALUE_SIZE_BYTES)
                .entries(DEFAULT_ENTRY_COUNT)
                .create());
    }

    public CompactingConcurrentMapMessageStore(final String name,
                                               final boolean removeNullPayloadMessages,
                                               final ConcurrentMap<String, MessageStoreEntry> messageMap) {
        this.name = name;
        this.entries = messageMap;
        this.removeNullPayloadMessages = removeNullPayloadMessages;
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
            if (entry.getTextMessage().getPayload() == null && removeNullPayloadMessages) {
                entries.remove(messageKey);
                compactedAndOrderedKeys.remove(messageKey);
            } else {
                entries.put(messageKey, entry);
                compactedAndOrderedKeys.add(messageKey);
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
            return compactedAndOrderedKeys
                    .stream()
                    .map(entries::get);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<MessageStoreEntry> stream(final String channelName) {
        lock.readLock().lock();
        try {
            return compactedAndOrderedKeys
                    .stream()
                    .map(entries::get)
                    .filter(entry->entry.getChannelName().equals(channelName));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        return entries.size();
    }
}
