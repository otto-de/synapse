package de.otto.synapse.messagestore;

import com.google.common.collect.EvictingQueue;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.copyOf;
import static de.otto.synapse.channel.ChannelPosition.*;

/**
 * Thread-safe in-memory implementation of a circular MessageStore that is storing all messages in insertion order
 * with a configurable capacity.
 *
 * <p>Each time an element is added to a full message store, the message store automatically removes its head element.
 */
@ThreadSafe
public class InMemoryRingBufferMessageStore implements WritableMessageStore {

    private final Queue<Message<String>> messages;
    private final AtomicReference<ChannelPosition> latestChannelPosition = new AtomicReference<>(fromHorizon());

    /**
     * Creates a new instance with default capacity of 100.
     */
    public InMemoryRingBufferMessageStore() {
        messages = EvictingQueue.create(100);
    }

    /**
     * Creates a new instance with specified capacity.
     *
     * @param capacity the size of the underlying ring buffer.
     */
    public InMemoryRingBufferMessageStore(final int capacity) {
        messages = EvictingQueue.create(capacity);
    }

    /**
     * Adds a Message to the MessageStore.
     *
     * <p>If the capacity of the ring buffer is reached, the oldest message is removed</p>
     * @param message the message to add
     */
    @Override
    public synchronized void add(final Message<String> message) {
        messages.add(message);
        latestChannelPosition.updateAndGet(previous -> {
            final Optional<ShardPosition> optionalMessagePosition = message.getHeader().getShardPosition();
            return optionalMessagePosition
                    .map(messagePosition -> merge(previous, channelPosition(messagePosition)))
                    .orElse(previous);
        });
    }

    /**
     * Returns the latest {@link ChannelPosition} of the MessageStore.
     * <p>
     *     The position is calculated by {@link ChannelPosition#merge(ChannelPosition...) merging} the
     *     {@link Header#getShardPosition() optional positions} of the messages.
     * </p>
     * <p>
     *     Messages without positions will not change the latest ChannelPosition. If no message contains
     *     position information, the returned ChannelPosition is {@link ChannelPosition#fromHorizon()}
     * </p>
     * @return ChannelPosition
     */
    @Override
    public synchronized ChannelPosition getLatestChannelPosition() {
        return latestChannelPosition.get();
    }

    /**
     * Returns a Stream of {@link Message messages} contained in the MessageStore.
     * <p>
     *     The stream will maintain the insertion order of the messages.
     * </p>
     *
     * @return Stream of messages
     */
    @Override
    public synchronized Stream<Message<String>> stream() {
        return copyOf(messages).stream();
    }

    @Override
    public synchronized int size() {
        return messages.size();
    }
}
