package de.otto.synapse.message;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
        return new Header(null, ImmutableMap.of());
    }

    public static Header requestHeader(final ImmutableMap<String, String> attributes) {
        return new Header(null, attributes);
    }

    public static Header responseHeader(final ShardPosition shardPosition,
                                        final ImmutableMap<String, String> attributes) {
        return new Header(shardPosition, attributes);
    }

    public static Header responseHeader(final ShardPosition shardPosition) {
        return new Header(shardPosition, ImmutableMap.of());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder copyOf(final Header header) {
        return new Builder()
                .withShardPosition(header.shardPosition)
                .withAttributes(header.getAll());
    }

    private final ShardPosition shardPosition;
    private final ImmutableMap<String, String> attributes;

    private Header(final ShardPosition shardPosition,
                   final ImmutableMap<String, String> attributes) {
        this.shardPosition = shardPosition;
        this.attributes = attributes;
    }

    @Nonnull
    public Optional<ShardPosition> getShardPosition() {
        return Optional.ofNullable(shardPosition);
    }

    @Nonnull
    @JsonAnyGetter
    public ImmutableMap<String, String> getAll() {
        return attributes;
    }

    public boolean containsKey(final String key) {
        return attributes.containsKey(key);
    }

    public boolean containsKey(final HeaderAttr attr) {
        return containsKey(attr.key());
    }

    @Nullable
    @JsonIgnore
    public Object get(final String key) {
        return attributes.get(key);
    }

    @Nullable
    @JsonIgnore
    public Object get(final HeaderAttr attr) {
        return get(attr.key());
    }

    @Nullable
    @JsonIgnore
    public Object get(final String key, final String defaultValue) {
        return attributes.getOrDefault(key, defaultValue);
    }

    @Nullable
    @JsonIgnore
    public Object get(final HeaderAttr attr, final String defaultValue) {
        return get(attr.key(), defaultValue);
    }

    @Nullable
    @JsonIgnore
    public String getAsString(final String key) {
        return Objects.toString(attributes.get(key), null);
    }

    @Nullable
    @JsonIgnore
    public String getAsString(final HeaderAttr attr) {
        return getAsString(attr.key());
    }

    @Nullable
    @JsonIgnore
    public String getAsString(final String key, final String defaultValue) {
        return Objects.toString(attributes.get(key), defaultValue);
    }

    @Nullable
    @JsonIgnore
    public String getAsString(final HeaderAttr attr, final String defaultValue) {
        return getAsString(attr.key(), defaultValue);
    }

    @Nullable
    @JsonIgnore
    public Instant getAsInstant(final String key) {
        return containsKey(key)
                ? Instant.parse(attributes.get(key))
                : null;
    }

    @Nullable
    @JsonIgnore
    public Instant getAsInstant(final HeaderAttr attr) {
        return getAsInstant(attr.key());
    }

    @Nullable
    @JsonIgnore
    public Instant getAsInstant(final String key, final Instant defaultValue) {
        return containsKey(key)
                ? Instant.parse(attributes.get(key))
                : defaultValue;
    }

    @Nullable
    @JsonIgnore
    public Instant getAsInstant(final HeaderAttr attr, final Instant defaultValue) {
        return getAsInstant(attr.key(), defaultValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Header header = (Header) o;
        return Objects.equals(shardPosition, header.shardPosition) &&
                Objects.equals(attributes, header.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardPosition, attributes);
    }

    @Override
    public String toString() {
        return "Header{" +
                "shardPosition=" + shardPosition +
                ", attributes=" + attributes +
                '}';
    }

    public static class Builder {
        private ShardPosition shardPosition;
        private final Map<String, String> attributes = new HashMap<>();

        public Builder withShardPosition(final @Nonnull ShardPosition shardPosition) {
            this.shardPosition = shardPosition;
            return this;
        }

        public Builder withAttribute(final @Nonnull String key, final @Nonnull String value) {
            this.attributes.put(key, value);
            return this;
        }

        public Builder withAttribute(final @Nonnull HeaderAttr attr, final @Nonnull String value) {
            return withAttribute(attr.key(), value);
        }

        public Builder withAttribute(final @Nonnull String key, final @Nonnull Instant value) {
            this.attributes.put(key, value.toString());
            return this;
        }

        public Builder withAttribute(final @Nonnull HeaderAttr attr, final @Nonnull Instant value) {
            return withAttribute(attr.key(), value);
        }

        public Builder withAttributes(final @Nonnull Map<String, String> attributes) {
            this.attributes.putAll(attributes);
            return this;
        }

        public Header build() {
            return new Header(shardPosition, ImmutableMap.copyOf(attributes));
        }
    }
}
