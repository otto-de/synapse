package de.otto.synapse.annotation;

import de.otto.synapse.configuration.MessageQueueReceiverEndpointAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;

import java.lang.annotation.*;


@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ImportAutoConfiguration({
        MessageQueueReceiverEndpointAutoConfiguration.class,
        MessageQueueReceiverEndpointBeanRegistrar.class})
@Repeatable(EnableMessageQueueReceiverEndpoints.class)
public @interface EnableMessageQueueReceiverEndpoint {

    /**
     * The name of the message queue.
     * <p>
     * Resolving placeholders like "${my.channel.name}" is supported for this property.
     * </p>
     *
     * @return channel name
     */
    String channelName();

    /**
     * The name of the registered MessageQueueReceiverEndpoint bean.
     * <p>
     * If {@code #name} is not set, the name of the bean is derived from the name of the message channel. The name
     * is constructed by tranforming hyphenated variable naming convention, e.g., "my-channel" into
     * the Spring bean naming convention, e.g., "myChannel". After this conversion, the string
     * "MessageQueueReceiverEndpoint" is appended. A channel named "my-channel" will therefore result in a bean name
     * "myChannelMessageQueueReceiverEndpoint".
     * </p>
     *
     * @return bean name
     */
    String name() default "";

}

