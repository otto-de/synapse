package de.otto.synapse.channel;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ChannelPositionTest {

    @Test
    public void shouldReturnZeroForUnknownShard() {
        final ChannelPosition channelPosition = ChannelPosition.fromHorizon();
        final String position = channelPosition.positionOf("unknown");
        // TODO: "0" used as magic value for "from horizon" position.
        assertThat(position, is("0"));
    }

    @Test
    public void shouldBuildFromHorizonStreamPosition() {
        final ChannelPosition channelPosition = ChannelPosition.fromHorizon();
        assertThat(channelPosition.shards(), is(empty()));
    }

    @Test
    public void shouldBuildSingleShardStreamPosition() {
        final ChannelPosition channelPosition = ChannelPosition.shardPosition("foo", "42");
        assertThat(channelPosition.shards(), contains("foo"));
        assertThat(channelPosition.positionOf("foo"), is("42"));
    }

    @Test
    public void shouldBuildStreamPositionFromMap() {
        final Map<String, String> shardPositions = new HashMap<String, String>() {{
            put("foo", "42");
            put("bar", "0815");
        }};
        final ChannelPosition channelPosition = ChannelPosition.of(shardPositions);
        assertThat(channelPosition.shards(), containsInAnyOrder("foo", "bar"));
        assertThat(channelPosition.positionOf("foo"), is("42"));
        assertThat(channelPosition.positionOf("bar"), is("0815"));
    }

    @Test
    public void shouldMergeDisjointStreamPositions() {
        final ChannelPosition first = ChannelPosition.shardPosition("foo", "42");
        final ChannelPosition second = ChannelPosition.shardPosition("bar", "4711");
        final ChannelPosition merged = ChannelPosition.merge(asList(first, second));
        assertThat(merged.shards(), containsInAnyOrder("foo", "bar"));
        assertThat(merged.positionOf("foo"), is("42"));
        assertThat(merged.positionOf("bar"), is("4711"));
    }

    @Test
    public void shouldKeepLastPositionForConflictingShards() {
        final ChannelPosition first = ChannelPosition.shardPosition("foo", "42");
        final ChannelPosition second = ChannelPosition.shardPosition("foo", "4711");
        final ChannelPosition merged = ChannelPosition.merge(asList(first, second));
        assertThat(merged.shards(), contains("foo"));
        assertThat(merged.positionOf("foo"), is("4711"));
    }
}