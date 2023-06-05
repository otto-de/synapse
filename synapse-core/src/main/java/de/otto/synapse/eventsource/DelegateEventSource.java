package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.BestMatchingSelectableComparator;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import jakarta.annotation.Nonnull;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static java.lang.String.format;

public class DelegateEventSource implements EventSource {

    private final EventSource delegate;

    public DelegateEventSource(final String messageLogBeanName,
                               final Class<? extends MessageLog> selector,
                               final List<EventSourceBuilder> eventSourceBuilder,
                               final ApplicationContext applicationContext) {
        final MessageLogReceiverEndpoint messageLogReceiverEndpoint = applicationContext.getBean(messageLogBeanName, MessageLogReceiverEndpoint.class);
        final EventSourceBuilder builder = eventSourceBuilder
                .stream()
                .filter(b -> b.matches(selector))
                .min(new BestMatchingSelectableComparator(selector))
                .orElseThrow(() -> new IllegalStateException(format("Unable to create EventSource for channelName=%s: no matching EventSourceBuilder found in the ApplicationContext.", messageLogReceiverEndpoint.getChannelName())));
        this.delegate = builder.buildEventSource(messageLogReceiverEndpoint);
    }

    public EventSource getDelegate() {
        return delegate;
    }

    /**
     * Registers a new EventConsumer at the EventSource.
     * <p>
     * {@link MessageConsumer consumers} have to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param messageConsumer the registered EventConsumer
     */
    @Override
    public void register(final MessageConsumer<?> messageConsumer) {
        delegate.register(messageConsumer);
    }

    /**
     * Returns registered EventConsumers.
     *
     * @return EventConsumers
     */
    @Override
    @Nonnull
    public MessageDispatcher getMessageDispatcher() {
        return delegate.getMessageDispatcher();
    }


    /**
     * Returns the MessageLogReceiverEndpoint used by the {@code EventSource} to consume events.
     *
     * @return MessageLogReceiverEndpoint
     */
    @Override
    @Nonnull
    public MessageLogReceiverEndpoint getMessageLogReceiverEndpoint() {
        return delegate.getMessageLogReceiverEndpoint();
    }

    /**
     * Returns the name of the EventSource.
     * <p>
     *     For streaming event-sources, this is the name of the event stream.
     * </p>
     *
     * @return name
     */
    @Override
    @Nonnull
    public String getChannelName() {
        return delegate.getChannelName();
    }

    /**
     * Consumes all events from the EventSource, until the (current) end of the stream is reached.
     * <p>
     *     The registered {@link MessageConsumer consumers} will be called zero or more times, depending on
     *     the number of events retrieved from the EventSource.
     * </p>
     *
     * @return the new read position
     */
    @Override
    public CompletableFuture<ChannelPosition> consume() {
        return delegate.consume();
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
    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull Predicate<ShardResponse> stopCondition) {
        return delegate.consumeUntil(stopCondition);
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public boolean isStopping() {
        return delegate.isStopping();
    }

    @Override
    public String toString() {
        return "DelegateEventSource{" +
                "delegate=" + delegate +
                '}';
    }
}
