package de.otto.synapse.aws.s3;

import de.otto.synapse.channel.ChannelPosition;

import java.time.Instant;
import java.util.function.Function;
import java.util.stream.Collectors;

// TODO: ChannelPosition -> final & diese hier löschen. Wenn der Timestamp benötigt wird, gehört das irgendwo anders hin
public class SnapshotChannelPosition extends ChannelPosition {

    private Instant snapshotTimestamp;

    private SnapshotChannelPosition(ChannelPosition channelPosition, Instant snapshotTimestamp) {
        super(channelPosition.shards().stream().collect(Collectors.toMap(Function.identity(), channelPosition::positionOf)));
        this.snapshotTimestamp = snapshotTimestamp;
    }

    public static SnapshotChannelPosition of() {
        return new SnapshotChannelPosition(ChannelPosition.fromHorizon(), null);
    }

    public static SnapshotChannelPosition of(ChannelPosition channelPosition, Instant snapshotTimestamp) {
        return new SnapshotChannelPosition(channelPosition, snapshotTimestamp);
    }

    public Instant getSnapshotTimestamp() {
        return snapshotTimestamp;
    }

    public void setSnapshotTimestamp(Instant snapshotTimestamp) {
        this.snapshotTimestamp = snapshotTimestamp;
    }
}
