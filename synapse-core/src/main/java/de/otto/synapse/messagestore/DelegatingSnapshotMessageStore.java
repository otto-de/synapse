package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableSet;
import de.otto.synapse.channel.ChannelPosition;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Stream;

public class DelegatingSnapshotMessageStore implements SnapshotMessageStore {
    private final MessageStore delegate;

    public DelegatingSnapshotMessageStore(final MessageStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public Instant getSnapshotTimestamp() {
        return delegate instanceof SnapshotMessageStore
                ? ((SnapshotMessageStore)delegate).getSnapshotTimestamp()
                : Instant.now();
    }

    @Override
    public Set<String> getChannelNames() {
        return delegate.getChannelNames();
    }

    @Override
    public ImmutableSet<Index> getIndexes() {
        return delegate.getIndexes();
    }

    @Override
    public ChannelPosition getLatestChannelPosition(String channelName) {
        return delegate.getLatestChannelPosition(channelName);
    }

    @Override
    @Deprecated
    public ChannelPosition getLatestChannelPosition() {
        return delegate.getLatestChannelPosition();
    }

    @Override
    public Stream<MessageStoreEntry> stream() {
        return delegate.stream();
    }

    @Override
    public Stream<MessageStoreEntry> stream(Index index, String value) {
        return delegate.stream(index, value);
    }

    /**
     * Guaranteed to throw an exception and leave the message store unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated    @Override
    public void add(@Nonnull MessageStoreEntry entry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
