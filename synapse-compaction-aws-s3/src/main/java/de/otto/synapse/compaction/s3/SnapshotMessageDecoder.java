package de.otto.synapse.compaction.s3;

import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.AbstractTextDecoder;

public class SnapshotMessageDecoder extends AbstractTextDecoder<SnapshotMessage> {

    @Override
    public TextMessage apply(final SnapshotMessage snapshotMessage) {
        return decode(snapshotMessage.getKey(), snapshotMessage.getHeader(), snapshotMessage.getPayload());
    }

}
