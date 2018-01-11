package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;

import javax.annotation.concurrent.ThreadSafe;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

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

    static <T> EventConsumer<T> of(final String keyPattern,
                                   final Class<T> payloadType,
                                   final Consumer<Event<T>> consumer) {
        return new EventConsumer<T>() {

            private Pattern pattern = compile(keyPattern);

            @Override
            public Class<T> payloadType() {
                return payloadType;
            }

            @Override
            public Pattern keyPattern() {
                return pattern;
            }

            @Override
            public void accept(Event<T> tEvent) {
                consumer.accept(tEvent);
            }
        };
    }

    /**
     * Returns the expected payload type of {@link Event events} consumed by this EventConsumer.
     *
     * @return payload type
     */
    Class<T> payloadType();

    /**
     * Returns the pattern of {@link Event#key() event keys} accepted by this consumer.
     *
     * @return Pattern
     */
    Pattern keyPattern();

}
