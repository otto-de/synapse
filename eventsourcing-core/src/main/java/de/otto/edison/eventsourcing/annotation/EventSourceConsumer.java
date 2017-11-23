package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventSource;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EventSourceConsumer {

    /**
     * The name of the registered EventConsumer bean.
     *
     * @return bean name
     */
    String name();

    /**
     * The name of the consumed event stream.
     * <p>
     *     Resolving placeholders like "${my.stream.name}" is supported for this property.
     * </p>
     * @return stream name
     */
    String streamName() default "";

    /**
     * The regex pattern to filter events by their key that the consumer should receive.
     * @return key pattern; defaults to <code>.*</code>
     */
    String keyPattern() default ".*";

    /**
     * The type of the {@link Event#payload} produced by the {@link EventSource}
     *
     * @return payload type
     */
    Class<?> payloadType();
}
