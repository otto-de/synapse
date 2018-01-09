package de.otto.edison.eventsourcing;

import de.otto.edison.eventsourcing.consumer.EventSource;

/**
 * A builder used to build {@link EventSource instances}.
 */
public interface EventSourceBuilder {

    /**
     * The name of the event stream.
     *
     * @param streamName event stream
     * @return EventSource
     */
    EventSource buildEventSource(final String streamName);

}
