package de.otto.synapse.messagestore;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableSet;
import de.otto.synapse.channel.ChannelPosition;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Thread-safe in-memory implementation of a circular MessageStore that is storing all entries in insertion order
 * with a configurable capacity.
 *
 * <p>Each time an element is added to a full message store, the message store automatically removes its head element.
 *
 * <p>This implementation does not support indexing of entries.</p>
 */
@ThreadSafe
public class InMemoryRingBufferMessageStore implements MessageStore {

    private final String name;
    private final Queue<MessageStoreEntry> entries;
    private final InMemoryChannelPositions channelPositions = new InMemoryChannelPositions();

    /**
     * Creates a new instance with default capacity of 100.
     *
     * @param name the name of the message store
     */
    public InMemoryRingBufferMessageStore(final String name) {
        this.name = name;
        this.entries = EvictingQueue.create(100);
    }

    /**
     * Creates a new instance with specified capacity.
     *
     * @param name the name of the message store
     * @param capacity the size of the underlying ring buffer.
     */
    public InMemoryRingBufferMessageStore(final String name,
                                          final int capacity) {
        this.name = name;
        this.entries = EvictingQueue.create(capacity);
    }

    @Override
    public synchronized void add(final MessageStoreEntry entry) {
        entries.add(entry);
        channelPositions.updateFrom(entry);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public synchronized Set<String> getChannelNames() {
        return channelPositions.channelNames();
    }

    @Override
    public ImmutableSet<Index> getIndexes() {
        return ImmutableSet.of();
    }

    @Override
    public synchronized ChannelPosition getLatestChannelPosition(final String channelName) {
        return channelPositions.positionOf(channelName);
    }

    @Override
    public synchronized Stream<MessageStoreEntry> stream() {
        return entries.stream();
    }

    @Override
    public Stream<MessageStoreEntry> stream(Index index, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int size() {
        return entries.size();
    }
}
