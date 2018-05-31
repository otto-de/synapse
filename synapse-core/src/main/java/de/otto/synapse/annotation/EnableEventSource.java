package de.otto.synapse.annotation;

import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Enables auto-configuration of {@link EventSource event sources}.
 * <p>
 *     A Spring bean with type {@code EventSource} is registered at the ApplicationContext. The name of the
 *     beans can be specified by {@link #name()}.
 * </p>
 * <p>
 *     EventSources are created using a {@link EventSourceBuilder} that must be registered in the
 *     {@code ApplicationContext}. The default builder is configured in
 *     {@link de.otto.synapse.configuration.SynapseAutoConfiguration} and will create instances of
 *     {@link de.otto.synapse.eventsource.DefaultEventSource}.
 * </p>
 * <p>
 *     Because the {@code EventSourceBuilder} is annotated as {@link ConditionalOnMissingBean}, it can be
 *     replaced by other implementations by just registering a different bean.
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

}
