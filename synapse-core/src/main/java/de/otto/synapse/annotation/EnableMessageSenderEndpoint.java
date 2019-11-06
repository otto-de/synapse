package de.otto.synapse.annotation;

import de.otto.synapse.channel.selector.MessageQueue;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.translator.MessageFormat;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;


@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({MessageSenderEndpointBeanRegistrar.class, SynapseAutoConfiguration.class})
@Repeatable(EnableMessageSenderEndpoints.class)
public @interface EnableMessageSenderEndpoint {

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
     * The name of the registered {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint} bean.
     * <p>
     * If {@code #name} is not set, the name of the bean is derived from the name of the message channel. The name
     * is constructed by tranforming hyphenated variable naming convention, e.g., "my-channel" into
     * the Spring bean naming convention, e.g., "myChannel". After this conversion, the string
     * "MessageQueueSenderEndpoint" is appended. A channel named "my-channel" will therefore result in a bean name
     * "myChannelMessageQueueSenderEndpoint".
     * </p>
     *
     * @return bean name
     */
    String name() default "";

    /**
     * The {@link MessageFormat} for the message to be sent.
     * Default: V1
     *
     * @return message format
     */
    MessageFormat messageFormat() default MessageFormat.V1;

    /**
     * Selector used to select one of possibly multiple available
     * {@link de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory} instances.
     *
     * <p>
     * Example: the SqsMessageSenderEndpointFactory matches both {@link MessageQueue MessageQueue.class}
     * and Sqs.class. The following usage of the annotation is selecting the SqsMessageSenderEndpointFactory
     * using the more specific SQS selector class:
     * </p>
     * <pre><code>
     * {@literal @}Configuration
     * {@literal @}EnableMessageSenderEndpoint(
     *      channelName = "some-sqs-queue",
     *      selector = Sqs.class)
     * class MyExampleConfiguration {
     * }
     * </code></pre>
     *
     * @return Selector class
     */
    Class<? extends Selector> selector();

}

