package de.otto.synapse.channel;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;

import static com.google.common.base.Functions.constant;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

@ThreadSafe
public final class ChannelDurationBehind implements Serializable {

    private static final Duration MAX_DURATION = Duration.ofMillis(Long.MAX_VALUE);
    private static final Duration UNKNOWN_DURATION_BEHIND = Duration.ofMillis(Long.MAX_VALUE);

    private final ImmutableMap<String, Duration> shardDurationBehind;

    private ChannelDurationBehind() {
        this.shardDurationBehind = ImmutableMap.of();
    }

    private ChannelDurationBehind(final @Nonnull Builder builder) {
        this.shardDurationBehind = ImmutableMap.copyOf(builder.shards);
    }

    public static ChannelDurationBehind unknown() {
        return new ChannelDurationBehind();
    }

    public static ChannelDurationBehind unknown(final @Nonnull Collection<String> shardNames) {
        return channelDurationBehind()
                .withAll(shardNames.stream().collect(toMap(identity(), constant(UNKNOWN_DURATION_BEHIND))))
                .build();
    }

    @Nonnull
    public Duration getDurationBehind() {
        return shardDurationBehind.values().stream()
                .max(Duration::compareTo)
                .orElse(MAX_DURATION);
    }

    @Nonnull
    public ImmutableMap<String, Duration> getShardDurationsBehind() {
        return ImmutableMap.copyOf(shardDurationBehind);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChannelDurationBehind that = (ChannelDurationBehind) o;
        return Objects.equals(shardDurationBehind, that.shardDurationBehind);
    }

    @Override
    public int hashCode() {

        return Objects.hash(shardDurationBehind);
    }

    @Override
    public String toString() {
        return "ChannelDurationBehind{" +
                "shardDurationBehind=" + shardDurationBehind +
                '}';
    }

    public static Builder channelDurationBehind() {
        return new Builder();
    }

    public static Builder copyOf(final @Nonnull ChannelDurationBehind prototype) {
        final Builder builder = channelDurationBehind();
        prototype.shardDurationBehind.forEach(builder::with);
        return builder;
    }

    public static class Builder {
        private final Map<String, Duration> shards = new LinkedHashMap<>();

        public Builder with(final @Nonnull String shardName,
                            final @Nonnull Duration behind) {
            shards.put(shardName, behind);
            return this;
        }

        public Builder withUnknown(final @Nonnull String shardName) {
            shards.put(shardName, UNKNOWN_DURATION_BEHIND);
            return this;
        }

        public Builder withAll(final Map<String, Duration> shards) {
            this.shards.putAll(shards);
            return this;
        }

        public Builder withAllUnknown(final Set<String> shardNames) {
            shardNames.forEach(this::withUnknown);
            return this;
        }

        public Builder without(final @Nonnull String shardName) {
            shards.remove(shardName);
            return this;
        }

        public ChannelDurationBehind build() {
            return new ChannelDurationBehind(this);
        }
    }
}
