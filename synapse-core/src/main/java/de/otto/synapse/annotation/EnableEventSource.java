package de.otto.synapse.annotation;

import de.otto.synapse.eventsource.EventSource;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Enables auto-configuration of {@link EventSource event sources}.
 * <p>
 *     A Spring bean with type {@code EventSource} is registered at the ApplicationContext. The name of the
 *     beans is specified by {@link #name()}.
 * </p>
 * <p>
 *     The {@link EventSource#getChannelName()} is configured using {@link #streamName()}. If {@code streamName} is not set, the
 *     name of the {@link #name() bean} is used instead.
 * </p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(EventSourceBeanRegistrar.class)
@Repeatable(EnableEventSources.class)
public @interface EnableEventSource {

    /**
     * The name of the registered EventSource bean.
     * <p>
     *     If {@link #name} is not set, the name of the bean is derived from the name of the stream.
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
    String streamName();

    /**
     * The optional name of the EventSourceBuilder bean used to construct the EventSource instance.
     * <p>
     *     By default, the bean named 'defaultEventSourceBuilder' is used.
     * </p>
     * @return bean name
     */
    String builder() default "defaultEventSourceBuilder";

}
