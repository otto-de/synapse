package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import de.otto.synapse.channel.ChannelPosition;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.slf4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static de.otto.synapse.messagestore.Indexers.noOpIndexer;
import static java.lang.Math.toIntExact;
import static org.mapdb.DBMaker.memoryDB;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Concurrent in-memory implementation of a MessageStore that is storing all messages in insertion order.
 */
@ThreadSafe
public class MapDbMessageStore implements MessageStore {

    private static final Logger LOG = getLogger(MapDbMessageStore.class);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ChannelPositions channelPositions = new ChannelPositions();
    private final Indexer indexer;
    private final BTreeMap<Long,MessageStoreEntry> entries;
    private final DB mapDb;
    private final AtomicLong nextKey = new AtomicLong();

    public MapDbMessageStore() {
        this.indexer = noOpIndexer();
        this.mapDb = memoryDB()
                .make();
        this.entries = (BTreeMap<Long,MessageStoreEntry>) mapDb
                .treeMap("entries")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
    }

    @Override
    public void add(final MessageStoreEntry entry) {
        lock.writeLock().lock();
        try {
            final MessageStoreEntry indexedEntry = indexer.index(entry);
            final String internalKey = entry.getChannelName() + ":" + entry.getTextMessage().getKey().compactionKey();
            entries.put(nextKey.getAndIncrement(), entry);
            channelPositions.updateFrom(indexedEntry);
            mapDb.commit();
        } catch (final Throwable e) {
            LOG.error("Exception caught while updating message store: " + e.getMessage(), e);
            mapDb.rollback();
            throw e;
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
        return indexer.getIndexes();
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
            return Streams
                    .stream(entries.valueIterator());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<MessageStoreEntry> stream(Index index, String value) {
        return Stream.empty();
    }

    @Override
    public int size() {
        return toIntExact(nextKey.get());
    }

    @Override
    public void close() {
        mapDb.close();
    }

    private String indexKeyOf(Index index, String value) {
        return index.getName() + "#" + value;
    }
}
