package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.TextMessage;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static de.otto.synapse.channel.ChannelPosition.*;

/**
 * Concurrent in-memory implementation of a MessageStore that is storing all messages in insertion order.
 */
@ThreadSafe
public class InMemoryMessageStore implements WritableMessageStore {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Deque<TextMessage> messages = new ConcurrentLinkedDeque<>();
    private final AtomicReference<ChannelPosition> latestChannelPosition = new AtomicReference<>(fromHorizon());

    @Override
    public void add(final TextMessage message) {
        lock.writeLock().lock();
        try {
            messages.add(message);
            latestChannelPosition.updateAndGet(previous -> {
                final Optional<ShardPosition> optionalMessagePosition = message.getHeader().getShardPosition();
                return optionalMessagePosition
                        .map(messagePosition -> merge(previous, channelPosition(messagePosition)))
                        .orElse(previous);
            });
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
            return messages.stream();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        return messages.size();
    }
}
