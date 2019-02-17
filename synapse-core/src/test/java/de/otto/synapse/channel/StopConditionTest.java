package de.otto.synapse.channel;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.testsupport.TestClock;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Predicate;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.ShardResponse.shardResponse;
import static de.otto.synapse.channel.StopCondition.*;
import static de.otto.synapse.message.DefaultHeaderAttr.MSG_ARRIVAL_TS;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static java.time.Instant.now;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class StopConditionTest {

    @Test
    public void shouldNeverStop() {
        final ShardResponse shardResponse = someEmptyShardResponse(ofMillis(1000));
        final Predicate<ShardResponse> predicate = shutdown();
        final boolean shouldStop = predicate.test(shardResponse);
        assertThat(shouldStop, is(false));
    }

    @Test
    public void shouldStopAtEndOfChannel() {
        final Predicate<ShardResponse> predicate = endOfChannel();
        assertThat(predicate.test(someShardResponseWithSingleMessage(ofMillis(1))), is(false));
        assertThat(predicate.test(someShardResponseWithSingleMessage(ZERO)), is(true));
    }

    @Test
    public void shouldStopAtEndOfChannelWithNoMoreMessages() {
        final Predicate<ShardResponse> predicate = endOfChannel().and(emptyResponse());
        assertThat(predicate.test(someShardResponseWithSingleMessage(ZERO)), is(false));
        assertThat(predicate.test(someEmptyShardResponse(ZERO)), is(true));
    }

    @Test
    public void shouldStopOnEmptyResponse() {
        final Predicate<ShardResponse> predicate = emptyResponse();
        assertThat(predicate.test(someShardResponseWithSingleMessage(ofMillis(42))), is(false));
        assertThat(predicate.test(someEmptyShardResponse(ofMillis(42))), is(true));
    }

    @Test
    public void shouldNotStopOnEmptyMessages() {
        final Predicate<ShardResponse> predicate = arrivalTimestampAfterNow();
        final ShardResponse emptyResponse = someEmptyShardResponse(ZERO);
        assertThat(predicate.test(emptyResponse), is(false));
    }

    @Test
    public void shouldNotStopOnMessageArrivedInPast() {
        final Predicate<ShardResponse> predicate = arrivalTimestampAfterNow();
        final Instant past = now().minus(1, ChronoUnit.HOURS);
        final ShardResponse pastResponse = someShardResponseWithMessages(
                ofMillis(42),
                TextMessage.of(
                        "foo",
                        arrivalTime(past),
                        "bar"));
        assertThat(predicate.test(pastResponse), is(false));
    }

    @Test
    public void shouldStopOnMessageArrivedAfterNow() {
        final Predicate<ShardResponse> predicate = arrivalTimestampAfterNow();
        final Instant future = now().plus(1, ChronoUnit.HOURS);
        final ShardResponse futureResponse = someShardResponseWithMessages(
                ofMillis(42),
                TextMessage.of("foo", arrivalTime(future), "bar"));
        assertThat(predicate.test(futureResponse), is(true));
    }

    @Test
    public void shouldStopInFuture() {
        final TestClock clock = TestClock.now();
        final Instant inThousandMillis = clock.instant().plusMillis(1000);

        final ShardResponse shardResponse = someEmptyShardResponse(ofMillis(1000));

        // when timestamp in future:
        final Predicate<ShardResponse> predicate = timestamp(inThousandMillis, clock);

        assertThat(predicate.test(shardResponse), is(false));

        // when stop-time is almost reached
        clock.proceed(1000, ChronoUnit.MILLIS);
        assertThat(predicate.test(shardResponse), is(false));

        // when stop-time is exactly reached
        clock.proceed(1, ChronoUnit.MILLIS);
        assertThat(predicate.test(shardResponse), is(true));
    }

    private ShardResponse someEmptyShardResponse(final Duration durationBehind) {
        return someShardResponseWithMessages(durationBehind);
    }

    private ShardResponse someShardResponseWithSingleMessage(final Duration durationBehind) {
        return someShardResponseWithMessages(
                durationBehind,
                TextMessage.of("foo", "bar"));
    }

    private ShardResponse someShardResponseWithMessages(final Duration durationBehind,
                                                        final TextMessage... messages) {
        return shardResponse(
                fromPosition("some-shard", "42"),
                durationBehind,
                messages);
    }

    private Header arrivalTime(final Instant arrivalTime) {
        return Header.of(fromPosition("shard-name", "42"), ImmutableMap.of(MSG_ARRIVAL_TS.key(), arrivalTime.toString()));
    }
}