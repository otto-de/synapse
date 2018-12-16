package de.otto.synapse.channel;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysFalse;

/**
 * Conditions used in {@link de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint receiver endpoints} to
 * specify under which circumstances message consumption should stop.
 *
 */
public final class StopCondition {

    /**
     * Do not stop message consumption until the application is shutting down.
     *
     * @return stop condition always returning false;
     */
    public static Predicate<ShardResponse> shutdown() {
        return alwaysFalse();
    }

    /**
     * Returns a stop condition that is true after the given timestamp.
     *
     * @param timestamp the point in time after which the predicate will return true.
     *
     * @return stop condition
     */
    public static Predicate<ShardResponse> timestamp(final Instant timestamp) {
        return timestamp(timestamp, Clock.systemDefaultZone());
    }

    /**
     * Returns a stop condition that is true after the given timestamp.
     *
     * @param timestamp the point in time after which the predicate will return true.
     * @param clock the clock used to get the current time
     *
     * @return stop condition
     */
    public static Predicate<ShardResponse> timestamp(final Instant timestamp, final Clock clock) {
        return (_x) -> clock.instant().isAfter(timestamp);
    }

    /**
     * Returns a stop condition that is true if at least one message contained in the {@code ShardResponse} has arrived
     * after creating the predicate.
     *
     * @return stop condition
     */
    public static Predicate<ShardResponse> arrivalTimeAfterNow() {
        return arrivalTimeAfterNow(Clock.systemDefaultZone());
    }

    /**
     * Returns a stop condition that is true if at least one message contained in the {@code ShardResponse} has arrived
     * after creating the predicate.
     *
     * @param clock the clock used to get the current time
     *
     * @return stop condition
     */
    public static Predicate<ShardResponse> arrivalTimeAfterNow(final Clock clock) {
        final Instant now = clock.instant();
        return (response) -> response
                .getMessages()
                .stream()
                .anyMatch(m -> m.getHeader().getArrivalTimestamp().isAfter(now));
    }

    /**
     * Returns a stop condition that is true if the end-of-channel is reached.
     * <p>
     *     It is assumed that end-of-channel is reached if {@link ShardResponse#getDurationBehind()} is
     *     {@link Duration#ZERO}, even if {@link ShardResponse#getMessages()} is not empty.
     * </p>
     * <p>
     *     Note that using this predicate as a stop condition for sharded message logs, the message consumption
     *     will stop after all shards have reached the end of the channels at least once.
     * </p>
     *
     * @return stop condition
     */
    public static Predicate<ShardResponse> endOfChannel() {
        return (response) -> response.getDurationBehind().equals(Duration.ZERO);
    }

    /**
     * Returns a stop condition that is true if the {@link ShardResponse} does not contain any messages.
     * <p>
     *     An empty {@code ShardResponse} does not necessarily indicate that the end-of-channel is reached.
     *     Especially in case of Kinesis message logs it is possible that multiple {@code ShardResponses} contain
     *     no messages, while the {@link ShardResponse#getDurationBehind()} is greater {@code ZERO}.
     * </p>
     * <p>
     *     You might want to combine this predicate with {@link #endOfChannel()} like this:
     * </p>
     * <pre><code>
     *     StopCondition.endOfChannel().and(StopCondition.emptyResponse());
     * </code></pre>
     * <p>
     *     Note that using this predicate as a stop condition for sharded message logs, the message consumption
     *     will stop after all shards have reached the end of the channels at least once.
     * </p>
     *
     * @return stop condition
     */
    public static Predicate<ShardResponse> emptyResponse() {
        return (response) -> response.getMessages().isEmpty();
    }

    private StopCondition() {}
}
