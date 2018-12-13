package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import org.springframework.context.ApplicationContext;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class DelegateEventSource implements EventSource {

    private final EventSource delegate;

    public DelegateEventSource(final String messageLogBeanName,
                               final EventSourceBuilder eventSourceBuilder,
                               final ApplicationContext applicationContext) {
        final MessageLogReceiverEndpoint messageLogReceiverEndpoint = applicationContext.getBean(messageLogBeanName, MessageLogReceiverEndpoint.class);
        this.delegate = eventSourceBuilder.buildEventSource(messageLogReceiverEndpoint);
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
     * For streaming event-sources, this is the name of the event stream.
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
     * @param until the timestamp until the messages should be consumed
     * @return the new read position
     */
    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull Instant until) {
        return delegate.consumeUntil(until);
    }

    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> catchUp() {
        return delegate.catchUp();
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
