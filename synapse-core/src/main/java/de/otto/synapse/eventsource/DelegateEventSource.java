package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.message.Message;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.function.Predicate;

public class DelegateEventSource implements EventSource, ApplicationContextAware {

    private final String name;
    private final String channelName;
    private final String eventSourceBuilder;
    private EventSource delegate;

    public DelegateEventSource(final String name,
                               final String channelName,
                               final String eventSourceBuilder) {
        this.name = name;
        this.channelName = channelName;
        this.eventSourceBuilder = eventSourceBuilder;
    }

    @Override
    public String getName() {
        return name;
    }

    public EventSource getDelegate() {
        return delegate;
    }

    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        delegate = applicationContext
                .getBean(eventSourceBuilder, EventSourceBuilder.class)
                .buildEventSource(name, channelName);
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
    @Override
    public ChannelPosition consumeUntil(final ChannelPosition startFrom,
                                        final Instant until) {
        return delegate.consumeUntil(startFrom, until);
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
                "channelName='" + channelName + '\'' +
                ", eventSourceBuilder='" + eventSourceBuilder + '\'' +
                ", delegate=" + delegate +
                '}';
    }
}
