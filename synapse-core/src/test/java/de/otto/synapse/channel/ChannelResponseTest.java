package de.otto.synapse.channel;


import com.google.common.collect.ImmutableList;
import de.otto.synapse.message.Message;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.collect.ImmutableList.of;
import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Message.message;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ChannelResponseTest {

    private Instant now;

    @Test
    public void shouldCalculateChannelPosition() {
        final ChannelResponse response = new ChannelResponse(
                "foo",
                of(
                        new ShardResponse("foo", ImmutableList.of(), fromHorizon("foo"), Duration.ZERO, Duration.ZERO),
                        new ShardResponse("foo", ImmutableList.of(), fromPosition("bar", "42"), Duration.ZERO, Duration.ZERO)
                )
        );
        assertThat(response.getChannelPosition(), is(channelPosition(fromHorizon("foo"), fromPosition("bar", "42"))));
    }

    @Test
    public void shouldReturnMessages() {
        final Message<String> first = message("a", null);
        final Message<String> second = message("b", null);
        final ChannelResponse response = new ChannelResponse(
                "foo",
                of(
                        new ShardResponse("foo", of(first, second), fromHorizon("foo"), Duration.ZERO, Duration.ZERO)
                )
        );
        assertThat(response.getMessages(), contains(first, second));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToCreateResponseWithoutAnyShardResponses() {
        new ChannelResponse("foo", of());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToCreateResponseFromDifferentChannels() {
        new ChannelResponse(
                "foo",
                of(
                        new ShardResponse("foo", ImmutableList.of(), fromHorizon("foo"), Duration.ZERO, Duration.ZERO),
                        new ShardResponse("bar", ImmutableList.of(), fromHorizon("bar"), Duration.ZERO, Duration.ZERO)
                )
        );
    }

    @Test
    public void shouldCalculateDurationBehind() {
        final ChannelResponse response = new ChannelResponse("foo", of(
                someShardResponse("first", 42000L),
                someShardResponse("second", 0L))
        );

        final ChannelDurationBehind expectedDurationBehind = channelDurationBehind()
                .with("first", Duration.ofSeconds(42))
                .with("second", Duration.ofSeconds(0))
                .build();
        assertThat(response.getChannelDurationBehind(), is(expectedDurationBehind));
    }

    @Test
    public void shouldReturnShardNames() {
        final ChannelResponse response = new ChannelResponse("foo", of(
                someShardResponse("first", 0L),
                someShardResponse("second", 0L))
        );

        assertThat(response.getShardNames(), containsInAnyOrder("first", "second"));
    }

    private ShardResponse someShardResponse(final String shardName,
                                            final long millisBehind) {
        return new ShardResponse("foo", ImmutableList.of(), fromPosition(shardName, "5"), Duration.ofMillis(0L), Duration.ofMillis(millisBehind));
    }


}

