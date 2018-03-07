package de.otto.synapse.channel;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static de.otto.synapse.channel.ChannelPosition.*;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ChannelPositionTest {

    @Test
    public void shouldReturnZeroForUnknownShard() {
        final ChannelPosition channelPosition = fromHorizon();
        final String position = channelPosition.positionOf("unknown");
        // TODO: "0" used as magic value for "from horizon" position.
        assertThat(position, is("0"));
    }

    @Test
    public void shouldBuildFromHorizonStreamPosition() {
        final ChannelPosition channelPosition = fromHorizon();
        assertThat(channelPosition.shards(), is(empty()));
    }

    @Test
    public void shouldBuildSingleShardStreamPosition() {
        final ChannelPosition channelPosition = shardPosition("foo", "42");
        assertThat(channelPosition.shards(), contains("foo"));
        assertThat(channelPosition.positionOf("foo"), is("42"));
    }

    @Test
    public void shouldBuildStreamPositionFromMap() {
        final ImmutableMap<String, String> shardPositions = ImmutableMap.of(
                "foo", "42",
                "bar", "0815");
        final ChannelPosition channelPosition = channelPosition(shardPositions);
        assertThat(channelPosition.shards(), containsInAnyOrder("foo", "bar"));
        assertThat(channelPosition.positionOf("foo"), is("42"));
        assertThat(channelPosition.positionOf("bar"), is("0815"));
    }

    @Test
    public void shouldMergeDisjointStreamPositions() {
        final ChannelPosition first = shardPosition("foo", "42");
        final ChannelPosition second = shardPosition("bar", "4711");
        final ChannelPosition merged = merge(asList(first, second));
        assertThat(merged.shards(), containsInAnyOrder("foo", "bar"));
        assertThat(merged.positionOf("foo"), is("42"));
        assertThat(merged.positionOf("bar"), is("4711"));
    }

    @Test
    public void shouldKeepLastPositionForConflictingShards() {
        final ChannelPosition first = shardPosition("foo", "42");
        final ChannelPosition second = shardPosition("foo", "4711");
        final ChannelPosition merged = merge(asList(first, second));
        assertThat(merged.shards(), contains("foo"));
        assertThat(merged.positionOf("foo"), is("4711"));
    }
}