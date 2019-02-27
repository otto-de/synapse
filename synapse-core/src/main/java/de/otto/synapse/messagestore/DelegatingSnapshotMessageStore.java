package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;

import java.time.Instant;
import java.util.Set;
import java.util.stream.Stream;

public class DelegatingSnapshotMessageStore implements SnapshotMessageStore {
    private final MessageStore delegate;

    public DelegatingSnapshotMessageStore(final MessageStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getName() {
        return delegate.getName();
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
    public ChannelPosition getLatestChannelPosition(String channelName) {
        return delegate.getLatestChannelPosition(channelName);
    }

    @Override
    @Deprecated
    public ChannelPosition getLatestChannelPosition() {
        return delegate.getLatestChannelPosition();
    }

    @Override
    public Stream<MessageStoreEntry> streamAll() {
        return delegate.streamAll();
    }

    @Override
    public Stream<MessageStoreEntry> stream(String channelName) {
        return delegate.stream(channelName);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
