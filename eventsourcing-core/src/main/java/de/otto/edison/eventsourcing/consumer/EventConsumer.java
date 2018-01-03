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
public interface EventConsumer<T> extends Consumer<Event<T>> {

    static <T> EventConsumer<T> of(final String streamName, final Consumer<Event<T>> consumer) {
        return new EventConsumer<T>() {
            @Override
            public String streamName() {
                return streamName;
            }

            @Override
            public void accept(Event<T> tEvent) {
                consumer.accept(tEvent);
            }
        };
    }

    /**
     * Returns the name of the consumed {@link EventSource}.
     * <p>
     * For streaming event-sources, this is the name of the event stream.
     * </p>
     *
     * @return event stream
     */
    String streamName();

}
