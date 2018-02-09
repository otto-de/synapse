package de.otto.edison.eventsourcing.aws.s3;

import de.otto.edison.eventsourcing.consumer.StreamPosition;

import java.time.Instant;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SnapshotStreamPosition extends StreamPosition {

    private Instant snapshotTimestamp;

    private SnapshotStreamPosition(StreamPosition streamPosition, Instant snapshotTimestamp) {
        super(streamPosition.shards().stream().collect(Collectors.toMap(Function.identity(), streamPosition::positionOf)));
        this.snapshotTimestamp = snapshotTimestamp;
    }

    public static SnapshotStreamPosition of() {
        return new SnapshotStreamPosition(StreamPosition.of(), null);
    }

    public static SnapshotStreamPosition of(StreamPosition streamPosition, Instant snapshotTimestamp) {
        return new SnapshotStreamPosition(streamPosition, snapshotTimestamp);
    }

    public Instant getSnapshotTimestamp() {
        return snapshotTimestamp;
    }

    public void setSnapshotTimestamp(Instant snapshotTimestamp) {
        this.snapshotTimestamp = snapshotTimestamp;
    }
}
