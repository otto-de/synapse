package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.channel.StopCondition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static de.otto.synapse.channel.StopCondition.shutdown;

/**
 * An event source of {@link Message events}.
 * <p>
 *     Event sources can be consumed by {@link Consumer consumers}.
 * </p>
 *
 */
public interface EventSource {

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
    default CompletableFuture<ChannelPosition> consume() {
        return consumeUntil(shutdown());
    }

    /**
     * Consumes all events from the EventSource until the timestamp is reached.
     * <p>
     *     The registered {@link MessageConsumer consumers} will be called zero or more times, depending on
     *     the number of events retrieved from the EventSource.
     * </p>
     *
     * @param stopCondition the stop condition used to determine whether or not message-consumption should stop
     * @return the new read position
     */
    @Nonnull CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull Predicate<ShardResponse> stopCondition);

    void stop();

    boolean isStopping();
}
