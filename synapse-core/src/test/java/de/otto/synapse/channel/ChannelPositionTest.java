package de.otto.synapse.channel;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.time.Duration;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.merge;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofHours;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ChannelPositionTest {

    @Test
    public void shouldReturnFromHorizonForUnknownShard() {
        final ChannelPosition channelPosition = channelPosition(fromPosition("foo", ZERO, "42"));
        ShardPosition shardPosition = channelPosition.shard("unknown");
        assertThat(shardPosition.startFrom(), is(StartFrom.HORIZON));
        assertThat(shardPosition.position(), is(""));
    }

    @Test
    public void shouldBuildFromHorizonStreamPosition() {
        final ChannelPosition channelPosition = ChannelPosition.fromHorizon();
        assertThat(channelPosition.shards(), is(empty()));
        assertThat(channelPosition.shard("foo"), is(fromHorizon("foo")));
    }

    @Test
    public void shouldBuildSingleShardStreamPosition() {
        final ChannelPosition channelPosition = channelPosition(fromPosition("foo", ZERO, "42"));
        assertThat(channelPosition.shards(), contains("foo"));
        assertThat(channelPosition.shard("foo").position(), is("42"));
        assertThat(channelPosition.shard("foo").startFrom(), is(StartFrom.POSITION));
    }

    @Test
    public void shouldBuildStreamPositionFromShards() {
        final ImmutableList<ShardPosition> shardPositions = ImmutableList.of(
                fromPosition("foo", ZERO, "42"),
                fromPosition("bar", ZERO, "0815"),
                fromHorizon("foobar"));
        final ChannelPosition channelPosition = channelPosition(shardPositions);
        assertThat(channelPosition.shards(), containsInAnyOrder("foo", "bar", "foobar"));
        assertThat(channelPosition.shard("foo"), is(fromPosition("foo", ZERO, "42")));
        assertThat(channelPosition.shard("bar"), is(fromPosition("bar", ZERO, "0815")));
        assertThat(channelPosition.shard("foobar").startFrom(), is(StartFrom.HORIZON));
        assertThat(channelPosition.shard("foobar").position(), is(""));
    }

    @Test
    public void shouldCalculateDurationBehindFromShards() {
        final ImmutableList<ShardPosition> shardPositions = ImmutableList.of(
                fromPosition("foo", Duration.ofHours(2), "42"),
                fromPosition("bar", Duration.ofHours(1), "0815"));
        final ChannelPosition channelPosition = channelPosition(shardPositions);
        assertThat(channelPosition.getDurationBehind(), is(ofHours(2)));
    }

    @Test
    public void shouldCalculateDurationAsMaxDurationForShardAtHorizon() {
        final ImmutableList<ShardPosition> shardPositions = ImmutableList.of(
                fromPosition("foo", Duration.ofHours(2), "42"),
                fromPosition("bar", Duration.ofHours(1), "0815"),
                fromHorizon("foobar"));
        final ChannelPosition channelPosition = channelPosition(shardPositions);
        assertThat(channelPosition.getDurationBehind(), is(Duration.ofMillis(Long.MAX_VALUE)));
    }

    @Test
    public void shouldCalculateDurationBehindForMergedShards() {
        final ChannelPosition first = channelPosition(fromPosition("foo", ofHours(1), "42"));
        final ChannelPosition second = channelPosition(fromPosition("bar", ofHours(2), "4711"));
        final ChannelPosition merged = merge(asList(first, second));
        assertThat(merged.getDurationBehind(), is(ofHours(2)));
    }

    @Test
    public void shouldMergeDisjointStreamPositions() {
        final ChannelPosition first = channelPosition(fromPosition("foo", ZERO, "42"));
        final ChannelPosition second = channelPosition(fromPosition("bar", ZERO, "4711"));
        final ChannelPosition merged = merge(asList(first, second));
        assertThat(merged.shards(), containsInAnyOrder("foo", "bar"));
        assertThat(merged.shard("foo"), is(fromPosition("foo", ZERO, "42")));
        assertThat(merged.shard("bar"), is(fromPosition("bar", ZERO, "4711")));
    }

    @Test
    public void shouldKeepLastPositionForConflictingShards() {
        final ChannelPosition first = channelPosition(fromPosition("foo", ZERO, "42"));
        final ChannelPosition second = channelPosition(fromPosition("foo", ZERO, "4711"));
        final ChannelPosition merged = merge(asList(first, second));
        assertThat(merged.shards(), contains("foo"));
        assertThat(merged.shard("foo").position(), is("4711"));
    }
}