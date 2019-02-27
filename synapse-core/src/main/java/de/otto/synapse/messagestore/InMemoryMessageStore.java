package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Concurrent in-memory implementation of a MessageStore that is storing all messages in insertion order.
 */
@ThreadSafe
public class InMemoryMessageStore implements MessageStore {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Deque<MessageStoreEntry> entries = new ConcurrentLinkedDeque<>();
    private final InMemoryChannelPositions channelPositions = new InMemoryChannelPositions();
    private final String name;

    public InMemoryMessageStore(final String name) {
        this.name = name;
    }

    @Override
    public void add(final MessageStoreEntry entry) {
        lock.writeLock().lock();
        try {
            entries.add(entry);
            channelPositions.updateFrom(entry);
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
            return entries.stream();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<MessageStoreEntry> stream(final String channelName) {
        lock.readLock().lock();
        try {
            return entries.stream().filter(e -> e.getChannelName().equals(channelName));
        } finally {
            lock.readLock().unlock();
        }
    }
    @Override
    public int size() {
        return entries.size();
    }
}
