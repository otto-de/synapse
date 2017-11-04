package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventSource;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Enables auto-configuration of {@link de.otto.edison.eventsourcing.consumer.EventSource event sources}.
 * <p>
 *     A Spring bean with type {@code EventSource} is registered at the ApplicationContext. The name of the
 *     beans is specified by {@link #name()}.
 * </p>
 * <p>
 *     The {@link EventSource#name()} is configured using {@link #streamName()}. If {@code streamName} is not set, the
 *     name of the {@link #name() bean} is used instead.
 * </p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(EnableEventSourceImportSelector.class)
@Repeatable(EnableEventSources.class)
public @interface EnableEventSource {

    /**
     * The name of the registered EventSource bean.
     * <p>
     *     If {@link #streamName} is not set, the name of the bean is used as
     *     the name of the stream.
     * </p>
     *
     * @return bean name
     */
    String name();

    /**
     * The name of the event stream.
     * <p>
     *     Resolving placeholders like "${my.stream.name}" is supported for this property.
     * </p>
     * @return stream name
     */
    String streamName() default "";

    /**
     * The type of the {@link Event#payload} produced by the {@link de.otto.edison.eventsourcing.consumer.EventSource}
     *
     * @return payload type
     */
    Class<?> payloadType();
}
