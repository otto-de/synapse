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
 * <p><em>Features:</em></p>
 * <ul>
 *     <li>Thread-Safe</li>
 *     <li>Support for maximum capacity.</li>
 *     <li>No support for compaction.</li>
 *     <li>No support for indexing of messages.</li>
 * </ul>
 */
@ThreadSafe
public class OnHeapRingBufferMessageStore implements MessageStore {

    private final Queue<MessageStoreEntry> entries;
    private final ChannelPositions channelPositions = new ChannelPositions();

    /**
     * Creates a new instance with default capacity of 100.
     *
     */
    public OnHeapRingBufferMessageStore() {
        this.entries = EvictingQueue.create(100);
    }

    /**
     * Creates a new instance with specified capacity.
     *
     * @param capacity the size of the underlying ring buffer.
     */
    public OnHeapRingBufferMessageStore(final int capacity) {
        this.entries = EvictingQueue.create(capacity);
    }

    @Override
    public synchronized void add(final MessageStoreEntry entry) {
        entries.add(entry);
        channelPositions.updateFrom(entry);
    }

    @Override
    public synchronized Set<String> getChannelNames() {
        return channelPositions.getChannelNames();
    }

    @Override
    public ImmutableSet<Index> getIndexes() {
        return ImmutableSet.of();
    }

    @Override
    public synchronized ChannelPosition getLatestChannelPosition(final String channelName) {
        return channelPositions.getLatestChannelPosition(channelName);
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
    public synchronized long size() {
        return entries.size();
    }
}
