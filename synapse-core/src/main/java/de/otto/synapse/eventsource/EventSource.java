package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;

/**
 * An event source of {@link Message events}.
 * <p>
 *     Event sources can be consumed by {@link Consumer consumers}.
 * </p>
 *
 */
public interface EventSource {

    /**
     * An event source's name is used to connect event sources to their consumers.
     * @return the unique name
     */
    String getName();

    /**
     * Registers a new EventConsumer at the EventSource.
     *
     * {@link MessageConsumer consumers} have to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param messageConsumer registered EventConsumer
     */
    void register(MessageConsumer<?> messageConsumer);

    /**
     * Returns the MessageDispatcher used by the EventSource to translate and sent incoming messages to the
     * registered {@link MessageConsumer message consumers}.
     *
     * @return MessageDispatcher
     */
    @Nonnull
    MessageDispatcher getMessageDispatcher();

    /**
     * Returns the MessageLogReceiverEndpoint used by the {@code EventSource} to consume events.
     *
     * @return MessageLogReceiverEndpoint
     */
    @Nonnull
    MessageLogReceiverEndpoint getMessageLogReceiverEndpoint();

    /**
     * Returns the name of the EventSource.
     * <p>
     *     For streaming event-sources, this is the name of the event stream.
     * </p>
     *
     * @return name
     */
    String getChannelName();

    /**
     * Consumes all events from the EventSource, until the (current) end of the stream is reached.
     * <p>
     *     The registered {@link MessageConsumer consumers} will be called zero or more times, depending on
     *     the number of events retrieved from the EventSource.
     * </p>
     *
     * @return the new read position
     */
    default ChannelPosition consume() {
        return consumeUntil(fromHorizon(), Instant.MAX);
    }

    /**
     * Consumes all events from the EventSource, beginning with {@link ChannelPosition startFrom}, until
     * the (current) end of the stream is reached.
     * <p>
     *     The registered {@link MessageConsumer consumers} will be called zero or more times, depending on
     *     the number of events retrieved from the EventSource.
     * </p>
     *
     * @param startFrom the read position returned from earlier executions
     * @return the new read position
     */
    default ChannelPosition consume(ChannelPosition startFrom) {
        return consumeUntil(startFrom, Instant.MAX);
    }

    /**
     * Consumes all events from the EventSource until the timestamp is reached.
     * <p>
     *     The registered {@link MessageConsumer consumers} will be called zero or more times, depending on
     *     the number of events retrieved from the EventSource.
     * </p>
     *
     * @param until the timestamp until the messages should be consumed
     * @return the new read position
     */
    default ChannelPosition consumeUntil(final @Nonnull Instant until) {
        return consumeUntil(fromHorizon(), until);
    }

    /**
     * Consumes all events from the EventSource, beginning with {@link ChannelPosition startFrom}, until
     * the {@link Predicate stopCondition} is met.
     * <p>
     *     The registered {@link MessageConsumer consumers} will be called zero or more times, depending on
     *     the number of events retrieved from the EventSource.
     * </p>
     *
     * @param startFrom the read position returned from earlier executions
     * @param until the arrival timestamp until the messages should be consumed
     * @return the new read position
     */
    @Nonnull ChannelPosition consumeUntil(@Nonnull ChannelPosition startFrom,
                                          @Nonnull Instant until);

    void stop();

    boolean isStopping();
}
