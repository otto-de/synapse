package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.message.EventBody;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EventSourceConsumer {

    /**
     * In some situations there might be multiple EventSource beans for a single event stream. In this
     * case, the eventSource attribute can be used to select one of the available beans.
     *
     * @return name of the EventSource bean to register the EventConsumer.
     */
    String eventSource();

    /**
     * The regex pattern to filter events by their key that the consumer should receive.
     *
     * @return key pattern; defaults to <code>.*</code>
     */
    String keyPattern() default ".*";

    /**
     * The type of the {@link EventBody#getPayload()} produced by the {@link EventSource}
     *
     * @return payload type
     */
    Class<?> payloadType();

}
