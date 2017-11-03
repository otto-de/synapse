package de.otto.edison.eventsourcing.consumer;

import java.util.function.Consumer;

@FunctionalInterface
public interface EventConsumer<T> extends Consumer<Event<T>> {

    /**
     * Called by an EventSource, if consumption of events is about to start.
     * <p>
     *     This method can be called multiple times for a single consumer and/or
     *     EventSource. For example, a composite EventSource could consist of multiple
     *     steps: for example, an EventSource could start reading a snapshot,
     *     followed by reading from an event stream.
     * </p>
     *
     * @param eventSource the name of the event source or event stream
     */
    default void init(final String eventSource) {
    }

    /**
     * Called by an EventSource, if consumption of events was sucessfully finished.
     * <p>
     *     This method can be called multiple times for a single consumer and/or
     *     EventSource. For example, a composite EventSource could consist of multiple
     *     steps: for example, an EventSource could start reading a snapshot,
     *     followed by reading from an event stream.
     * </p>
     *
     * @param eventSource the name of the event source or event stream
     */
    default void completed(final String eventSource) {
    }

    /**
     * Called by an EventSource, if consumption of events was aborted because of some error.
     *
     * @param eventSource the name of the event source or event stream
     */
    default void aborted(final String eventSource) {
    }

    void accept(Event<T> event);

}
