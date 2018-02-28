package de.otto.synapse.channel;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public final class ChannelResponse {

    private final Status status;
    private final ChannelPosition channelPosition;

    private ChannelResponse(final Status status,
                            final ChannelPosition channelPosition) {
        this.status = status;
        this.channelPosition = channelPosition;
    }

    public static ChannelResponse of(final Status status,
                                     final ChannelPosition channelPosition) {
        return new ChannelResponse(status, channelPosition);
    }

    public static ChannelResponse of(final List<ChannelResponse> channelRespons) {
        final boolean stopped = channelRespons
                .stream()
                .anyMatch(streamResponse -> streamResponse.getStatus() == Status.STOPPED);
        final List<ChannelPosition> channelPositions = channelRespons
                .stream()
                .map(ChannelResponse::getChannelPosition)
                .collect(toList());
        return ChannelResponse.of(
                stopped ? Status.STOPPED : Status.OK,
                ChannelPosition.merge(channelPositions));
    }

    public Status getStatus() {
        return status;
    }

    public ChannelPosition getChannelPosition() {
        return channelPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChannelResponse that = (ChannelResponse) o;
        return status == that.status &&
                Objects.equals(channelPosition, that.channelPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, channelPosition);
    }

    @Override
    public String toString() {
        return "StreamResponse{" +
                "status=" + status +
                ", streamPosition=" + channelPosition +
                '}';
    }
}
