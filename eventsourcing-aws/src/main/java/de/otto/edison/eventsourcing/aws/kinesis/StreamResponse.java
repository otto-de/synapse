package de.otto.edison.eventsourcing.aws.kinesis;

import de.otto.edison.eventsourcing.consumer.StreamPosition;

import java.util.Collection;
import java.util.Objects;

import static java.util.stream.Collectors.toMap;

public final class StreamResponse {

    private final Status status;
    private final StreamPosition streamPosition;

    private StreamResponse(final Status status,
                           final StreamPosition streamPosition) {
        this.status = status;
        this.streamPosition = streamPosition;
    }

    public static StreamResponse of(final Status status,
                                    final StreamPosition streamPosition) {
        return new StreamResponse(status, streamPosition);
    }

    public static StreamResponse of(final Collection<ShardResponse> shardResponses) {
        final boolean stopped = shardResponses.stream().anyMatch(shardResponse -> shardResponse.getStatus() == Status.STOPPED);
        return StreamResponse.of(
                stopped ? Status.STOPPED : Status.OK,
                StreamPosition.of(
                        shardResponses
                        .stream()
                        .map(ShardResponse::getShardPosition)
                        .collect(toMap(ShardPosition::getShardId, ShardPosition::getSequenceNumber))
                )
        );
    }

    public Status getStatus() {
        return status;
    }

    public StreamPosition getStreamPosition() {
        return streamPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamResponse that = (StreamResponse) o;
        return status == that.status &&
                Objects.equals(streamPosition, that.streamPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, streamPosition);
    }

    @Override
    public String toString() {
        return "StreamResponse{" +
                "status=" + status +
                ", streamPosition=" + streamPosition +
                '}';
    }
}
