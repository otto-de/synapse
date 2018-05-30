package de.otto.synapse.annotation;

import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Enables auto-configuration of {@link EventSource event sources}.
 * <p>
 *     A Spring bean with type {@code EventSource} is registered at the ApplicationContext. The name of the
 *     beans can be specified by {@link #name()}.
 * </p>
 * <p>
 *     EventSources are created using one of the registered {@link EventSourceBuilder}.
 *     The {@link #builder()} attribute is used to select the {@code EventSourceBuilder} instance.
 * </p>
 * <p>
 *     The {@link EventSource#getChannelName()} is configured using {@link #channelName()}.
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
     *     If {@link #name} is not set, the name of the bean is derived from the name of the message channel.
     * </p>
     *
     * @return bean name
     */
    String name() default "";

    /**
     * The name of the message channel.
     * <p>
     *     Resolving placeholders like "${my.channel.name}" is supported for this property.
     * </p>
     * @return stream name
     */
    String channelName();

    /**
     * The optional name of the {@link EventSourceBuilder} bean used to construct the {@link EventSource} instance.
     * <p>
     *     By default, the bean named 'defaultEventSourceBuilder' is used.
     * </p>
     * @return bean name
     */
    String builder() default "defaultEventSourceBuilder";

}
