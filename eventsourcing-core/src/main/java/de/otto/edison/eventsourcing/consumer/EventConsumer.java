package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;

import javax.annotation.concurrent.ThreadSafe;
import java.util.function.Consumer;

/**
 * A consumer for Events with payload-type &lt;T&gt;.
 * <p>
 * Multiple EventConsumers may listen at a single {@link EventSource}. A single EventConsumer
 * must be registered to multiple EventSources.
 * </p>
 * <p>
 * By default, Edison-Eventsourcing is auto-configuring a {@link EventSourceConsumerProcess} that is
 * running a separate thread for every EventConsumer. The thread is taking care for continuous
 * consumption of events using the consumers, until the application is shutting down.
 * </p>
 * <p>
 * If you need to manually consume events using EventConsumers, auto-configuration of the
 * {@code EventSourceConsumerProcess} can be disabled by setting
 * {@link EventSourcingProperties#getConsumerProcess() edison.eventsourcing.consumer-process.enabled=false}
 * </p>
 * <p>
 * EventConsumers are expected to be thread-safe.
 * </p>
 *
 * @param <T> the type of the event's payload
 */
@ThreadSafe
public interface EventConsumer<T> {

    /**
     * Returns the name of the consumed {@link EventSource}.
     * <p>
     * For streaming event-sources, this is the name of the event stream.
     * </p>
     *
     * @return event stream
     */
    String streamName();

    /**
     * Consumer to consume a single event.
     *
     * @return consumer function that is called for each event
     */
    Consumer<Event<T>> consumerFunction();

    /**
     * The regex pattern to filter events by their key that the consumer should receive.
     * @return key pattern
     */
    String getKeyPattern();
}
