package de.otto.synapse.message;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

/**
 * The Header of a {@link Message}.
 * <p>
 *     Headers contain metadata about a message which may only be available on one side of a
 *     channel: {@link MessageLogReceiverEndpoint}, {@link MessageQueueReceiverEndpoint} or
 *     {@link AbstractMessageSenderEndpoint} might add different information to
 *     the message header.
 * </p>
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageSolution.gif" alt="Message">
 * </p>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html">EIP: Message</a>
 */
public class Header implements Serializable {

    public static Header emptyHeader() {
        return new Header(null, now(), ImmutableMap.of());
    }

    public static Header requestHeader(final ImmutableMap<String, Object> attributes) {
        return new Header(null, now(), attributes);
    }

    public static Header responseHeader(final ShardPosition shardPosition, final Instant arrivalTimestamp, final ImmutableMap<String, Object> attributes) {
        return new Header(shardPosition, arrivalTimestamp, attributes);
    }

    public static Header responseHeader(final ShardPosition shardPosition,
                                        final Instant arrivalTimestamp) {
        return new Header(shardPosition, arrivalTimestamp, ImmutableMap.of());
    }

    private final ShardPosition shardPosition;
    private final Instant arrivalTimestamp;
    private final ImmutableMap<String, Object> attributes;

    private Header(final ShardPosition shardPosition,
                   final Instant approximateArrivalTimestamp,
                   final ImmutableMap<String, Object> attributes) {
        this.shardPosition = shardPosition;
        this.arrivalTimestamp = requireNonNull(approximateArrivalTimestamp);
        this.attributes = attributes;
    }

    @Nonnull
    public Optional<ShardPosition> getShardPosition() {
        return Optional.ofNullable(shardPosition);
    }

    @Nonnull
    public Instant getArrivalTimestamp() {
        return arrivalTimestamp;
    }

    @Nonnull
    public ImmutableMap<String, Object> getAttributes() {
        return attributes;
    }

    public boolean hasAttribute(final String key) {
        return attributes.containsKey(key);
    }

    @Nullable
    @JsonIgnore
    public Object getAttribute(final String key) {
        return attributes.get(key);
    }

    @Nullable
    @JsonIgnore
    public String getStringAttribute(final String key) {
        return Objects.toString(attributes.get(key), null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Header header = (Header) o;
        return Objects.equals(shardPosition, header.shardPosition) &&
                Objects.equals(arrivalTimestamp, header.arrivalTimestamp) &&
                Objects.equals(attributes, header.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardPosition, arrivalTimestamp, attributes);
    }

    @Override
    public String toString() {
        return "Header{" +
                "shardPosition=" + shardPosition +
                ", arrivalTimestamp=" + arrivalTimestamp +
                ", attributes=" + attributes +
                '}';
    }

}
