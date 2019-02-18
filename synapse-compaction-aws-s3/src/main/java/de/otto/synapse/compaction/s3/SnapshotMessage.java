package de.otto.synapse.compaction.s3;

import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;

public class SnapshotMessage {
    private final Key key;
    private final Header header;
    private final String payload;

    public SnapshotMessage(final Key key,
                           final Header header,
                           final String payload) {
        this.key = key;
        this.header = header;
        this.payload = payload;
    }

    public Key getKey() {
        return key;
    }

    public Header getHeader() {
        return header;
    }

    public String getPayload() {
        return payload;
    }
}
