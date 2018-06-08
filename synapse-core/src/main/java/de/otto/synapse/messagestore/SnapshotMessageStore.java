package de.otto.synapse.messagestore;

import java.time.Instant;

public interface SnapshotMessageStore extends MessageStore {

    Instant getSnapshotTimestamp();

}
