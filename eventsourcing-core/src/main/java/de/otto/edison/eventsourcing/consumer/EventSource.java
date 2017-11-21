package de.otto.edison.eventsourcing.consumer;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * An event source of {@link Event events} with a payload type {@code T}.
 * <p>
 *     Event sources can be consumed by {@link Consumer consumers}.
 * </p>
 *
 * @param <T> the type of the event payloads
 */
public interface EventSource<T> {

    /**
     * Returns the name of the EventSource.
     * <p>
     *     For streaming event-sources, this is the name of the event stream.
     * </p>
     *
     * @return name
     */
    String getStreamName();

    /**
     * Consumes all events from the EventSource, until the (current) end of the stream is reached.
     * <p>
     *     The {@link Consumer consumer} will be called zero or more times, depending on
     *     the number of events retrieved from the EventSource.
     * </p>
     *
     * {@link Consumer consumer} has to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param consumer consumer used to process the events
     * @return the new read position
     */
    default StreamPosition consumeAll(Consumer<Event<T>> consumer) {
        return consumeAll(StreamPosition.of(), event -> false, consumer);
    }

    /**
     * Consumes all events from the EventSource, beginning with {@link StreamPosition startFrom}, until
     * the (current) end of the stream is reached.
     * <p>
     *     The {@link Consumer consumer} will be called zero or more times, depending on
     *     the number of events retrieved from the EventSource.
     * </p>
     *
     * {@link Consumer consumer} has to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param startFrom the read position returned from earlier executions
     * @param consumer consumer used to process the events
     * @return the new read position
     */
    default StreamPosition consumeAll(StreamPosition startFrom,
                                      Consumer<Event<T>> consumer) {
        return consumeAll(startFrom, event -> false, consumer);
    }

    /**
     * Consumes all events from the EventSource until the {@link Predicate stopCondition} is met.
     * <p>
     *     The {@link Consumer consumer} will be called zero or more times, depending on
     *     the number of events retrieved from the EventSource.
     * </p>
     *
     * {@link Consumer consumer} has to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param stopCondition the predicate used as a stop condition
     * @param consumer consumer used to process events
     * @return the new read position
     */
    default StreamPosition consumeAll(Predicate<Event<T>> stopCondition, Consumer<Event<T>> consumer) {
        return consumeAll(StreamPosition.of(), stopCondition, consumer);
    }

    /**
     * Consumes all events from the EventSource, beginning with {@link StreamPosition startFrom}, until
     * the {@link Predicate stopCondition} is met.
     * <p>
     *     The {@link Consumer consumer} will be called zero or more times, depending on
     *     the number of events retrieved from the EventSource.
     * </p>
     *
     * {@link Consumer consumer} has to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param startFrom the read position returned from earlier executions
     * @param stopCondition the predicate used as a stop condition
     * @param consumer consumer used to process events
     * @return the new read position
     */
    StreamPosition consumeAll(StreamPosition startFrom,
                              Predicate<Event<T>> stopCondition,
                              Consumer<Event<T>> consumer);
}
