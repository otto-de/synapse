package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;

import java.time.Instant;
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
    public ChannelPosition getLatestChannelPosition() {
        return delegate.getLatestChannelPosition();
    }

    @Override
    public Stream<Message<String>> stream() {
        return delegate.stream();
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
