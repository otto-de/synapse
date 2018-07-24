package de.otto.synapse.annotation.messagequeue;

import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;


@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(EnableMessageQueueReceiverEndpointBeanRegistrar.class)
@Repeatable(EnableMessageQueueReceiverEndpoints.class)
public @interface EnableMessageQueueReceiverEndpoint {

    /**
     * The name of the message queue.
     * <p>
     * Resolving placeholders like "${my.channel.name}" is supported for this property.
     * </p>
     *
     * @return stream name
     */
    String channelName();

    /**
     * The name of the registered EventSource bean.
     * <p>
     * If {@code #name} is not set, the name of the bean is derived from the name of the message channel. The name
     * is constructed by tranforming hyphenated variable naming convention, e.g., "my-channel" into
     * the Spring bean naming convention, e.g., "myChannel". After this conversion, the string "EventSource" is
     * appended. A channel named "my-channel" will therefore result in a bean name "myChannelEventSource".
     * </p>
     *
     * @return bean name
     */
    String name() default "";

    /**
     * The name of the {@link MessageLogReceiverEndpoint} bean that is used to create
     * the {@link MessageQueueReceiverEndpoint} bean.
     * <p>
     * If {@code messageLogReceiverEndpoint} is not set, the name of the bean is derived from the name of the
     * message channel. The name is constructed by tranforming hyphenated variable naming convention, e.g.,
     * "my-channel" into the Spring bean naming convention, e.g., "myChannel". After this conversion, the string
     * "MessageLogReceiverEndpoint" is appended. A channel named "my-channel" will therefore result in a
     * bean name "myChannelMessageLogReceiverEndpoint".
     * </p>
     * <p>
     * The {@link EnableMessageQueueReceiverEndpointBeanRegistrar} is responsible for creating the {@code EventSources} specified by this
     * annotation. The bean name, either specified or derived from the {@code channelName}, will be used by the
     * {@link EnableMessageQueueReceiverEndpointBeanRegistrar} as the name of the registered message log bean. If a bean having this name
     * already exists, a {@link BeanCreationException} will be thrown during startup.
     * </p>
     *
     * @return bean name of the {@code MessageQueueReceiverEndpoint}
     */
    String messageQueueReceiverEndpoint() default "";

}

