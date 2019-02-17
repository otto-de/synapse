package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ChannelPosition.merge;

/**
 * Concurrent implementation of a MessageStore that is compacting messages by {@link Message#getKey() of}.
 * <p>
 *     The ordering of messages is guaranteed by using a (on-heap) ConcurrentSkipListSet for keys.
 * </p>
 * <p>
 *     The messages are stored in a ConcurrentMap like, for example, ChronicleMap. This way, the messages
 *     can be stored off-heap, so large numbers of messages can be stored in memory, without getting problems
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
    private final ConcurrentMap<String, TextMessage> messages;
    private final AtomicReference<ChannelPosition> latestChannelPosition = new AtomicReference<>(fromHorizon());
    private final boolean removeNullPayloadMessages;

    public CompactingConcurrentMapMessageStore() {
        this(true);
    }

    public CompactingConcurrentMapMessageStore(final boolean removeNullPayloadMessages) {
        // TODO why ChronicleMap as a default??
        this(removeNullPayloadMessages, ChronicleMapBuilder.of(String.class, TextMessage.class)
                .averageKeySize(DEFAULT_KEY_SIZE_BYTES)
                .averageValueSize(DEFAULT_VALUE_SIZE_BYTES)
                .entries(DEFAULT_ENTRY_COUNT)
                .create());
    }

    public CompactingConcurrentMapMessageStore(final boolean removeNullPayloadMessages,
                                               final ConcurrentMap<String, TextMessage> messageMap) {
        this.messages = messageMap;
        this.removeNullPayloadMessages = removeNullPayloadMessages;
    }

    @Override
    public void add(final TextMessage message) {
        final String messageKey = message.getKey().compactionKey();
        lock.writeLock().lock();
        try {
            if (message.getPayload() == null && removeNullPayloadMessages) {
                messages.remove(messageKey);
                compactedAndOrderedKeys.remove(messageKey);
            } else {
                messages.put(messageKey, message);
                compactedAndOrderedKeys.add(messageKey);
            }
            latestChannelPosition.updateAndGet(previous -> message
                    .getHeader()
                    .getShardPosition()
                    .map(messageChannelPosition -> merge(previous, messageChannelPosition))
                    .orElse(previous)
            );
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public ChannelPosition getLatestChannelPosition() {
        lock.readLock().lock();
        try {
            return latestChannelPosition.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<TextMessage> stream() {
        lock.readLock().lock();
        try {
            return compactedAndOrderedKeys.stream().map(messages::get);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        return messages.size();
    }
}
