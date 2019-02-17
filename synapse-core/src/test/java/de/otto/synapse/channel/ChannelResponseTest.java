package de.otto.synapse.channel;


import com.google.common.collect.ImmutableList;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import org.junit.Test;

import java.time.Duration;

import static com.google.common.collect.ImmutableList.of;
import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.ShardResponse.shardResponse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ChannelResponseTest {

    @Test
    public void shouldCalculateChannelPosition() {
        final ChannelResponse response = ChannelResponse.channelResponse("foo",
                shardResponse(fromHorizon("foo"), Duration.ZERO, ImmutableList.of()),
                shardResponse(fromPosition("bar", "42"), Duration.ZERO, ImmutableList.of())
        );
        assertThat(response.getChannelPosition(), is(channelPosition(fromHorizon("foo"), fromPosition("bar", "42"))));
    }

    @Test
    public void shouldReturnMessages() {
        final TextMessage first = TextMessage.of(Key.of("a"), null);
        final TextMessage second = TextMessage.of(Key.of("b"), null);
        final ChannelResponse response = ChannelResponse.channelResponse(
                "foo",
                shardResponse(fromHorizon("foo"), Duration.ZERO, of(first, second))
        );
        assertThat(response.getMessages(), contains(first, second));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToCreateResponseWithoutAnyShardResponses() {
        ChannelResponse.channelResponse("foo", of());
    }

    @Test
    public void shouldCalculateDurationBehind() {
        final ChannelResponse response = ChannelResponse.channelResponse("foo", of(
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
        final ChannelResponse response = ChannelResponse.channelResponse("foo", of(
                someShardResponse("first", 0L),
                someShardResponse("second", 0L))
        );

        assertThat(response.getShardNames(), containsInAnyOrder("first", "second"));
    }

    private ShardResponse someShardResponse(final String shardName,
                                            final long millisBehind) {
        return shardResponse(fromPosition(shardName, "5"), Duration.ofMillis(millisBehind), ImmutableList.of());
    }


}

