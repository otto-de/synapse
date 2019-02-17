package de.otto.synapse.edison.trace;

import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.message.TextMessage;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class TraceEntry implements Serializable {
    private static AtomicLong nextSequenceNumber = new AtomicLong(0L);
    private final long sequenceNumber = nextSequenceNumber.getAndIncrement();
    private final Instant timestamp;
    private final String channelName;
    private final EndpointType endpointType;
    private final TextMessage message;

    public TraceEntry(final String channelName,
                      final EndpointType endpointType,
                      final TextMessage message) {
        this.timestamp = Instant.now();
        this.channelName = channelName;
        this.endpointType = endpointType;
        this.message = message;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getChannelName() {
        return channelName;
    }

    public EndpointType getEndpointType() {
        return endpointType;
    }

    public TextMessage getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraceEntry that = (TraceEntry) o;
        return sequenceNumber == that.sequenceNumber &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(channelName, that.channelName) &&
                endpointType == that.endpointType &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {

        return Objects.hash(sequenceNumber, timestamp, channelName, endpointType, message);
    }

    @Override
    public String toString() {
        return "TraceEntry{" +
                "sequenceNumber=" + sequenceNumber +
                ", timestamp=" + timestamp +
                ", channelName='" + channelName + '\'' +
                ", endpointType=" + endpointType +
                ", message=" + message +
                '}';
    }
}
