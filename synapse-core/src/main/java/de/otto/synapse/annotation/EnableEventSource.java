package de.otto.synapse.annotation;

import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import org.springframework.beans.factory.BeanCreationException;
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
@EnableEventSourcing
public @interface EnableEventSource {

    /**
     * The name of the message channel.
     * <p>
     *     Resolving placeholders like "${my.channel.name}" is supported for this property.
     * </p>
     * @return channel name
     */
    String channelName();

    /**
     * The name of the registered EventSource bean.
     * <p>
     *     If {@code #name} is not set, the name of the bean is derived from the name of the message channel. The name
     *     is constructed by tranforming hyphenated variable naming convention, e.g., "my-channel" into
     *     the Spring bean naming convention, e.g., "myChannel". After this conversion, the string "EventSource" is
     *     appended. A channel named "my-channel" will therefore result in a bean name "myChannelEventSource".
     * </p>
     *
     * @return bean name
     */
    String name() default "";

    /**
     * The name of the {@link MessageLogReceiverEndpoint} bean that is used to create
     * the {@link EventSource} bean.
     * <p>
     *     If {@code messageLogReceiverEndpoint} is not set, the name of the bean is derived from the name of the
     *     message channel. The name is constructed by tranforming hyphenated variable naming convention, e.g.,
     *     "my-channel" into the Spring bean naming convention, e.g., "myChannel". After this conversion, the string
     *     "MessageLogReceiverEndpoint" is appended. A channel named "my-channel" will therefore result in a
     *     bean name "myChannelMessageLogReceiverEndpoint".
     * </p>
     * <p>
     *     The {@link EventSourceBeanRegistrar} is responsible for creating the {@code EventSources} specified by this
     *     annotation. The bean name, either specified or derived from the {@code channelName}, will be used by the
     *     {@link EventSourceBeanRegistrar} as the name of the registered message log bean. If a bean having this name
     *     already exists, a {@link BeanCreationException} will be thrown during startup.
     * </p>
     *
     * @return bean name of the {@code MessageLogReceiverEndpoint}
     */
    String messageLogReceiverEndpoint() default "";

    /**
     *
     */
    StartFrom iteratorAt() default StartFrom.HORIZON;

}
