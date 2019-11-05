package de.otto.synapse.annotation;

import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.configuration.MessageLogReceiverEndpointAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;


@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({MessageLogReceiverEndpointBeanRegistrar.class, MessageLogReceiverEndpointAutoConfiguration.class})
@Repeatable(EnableMessageLogReceiverEndpoints.class)
public @interface EnableMessageLogReceiverEndpoint {

    /**
     * The name of the message-log channel.
     * <p>
     * Resolving placeholders like "${my.channel.name}" is supported for this property.
     * </p>
     *
     * @return channel name
     */
    String channelName();

    /**
     * The name of the registered MessageLogReceiverEndpoint bean.
     * <p>
     * If {@code #name} is not set, the name of the bean is derived from the name of the message channel. The name
     * is constructed by tranforming hyphenated variable naming convention, e.g., "my-channel" into
     * the Spring bean naming convention, e.g., "myChannel". After this conversion, the string
     * "MessageLogReceiverEndpoint" is appended. A channel named "my-channel" will therefore result in a bean name
     * "myChannelMessageLogReceiverEndpoint".
     * </p>
     *
     * @return bean name
     */
    String name() default "";

    /**
     * Specifies where to start reading from the message log.
     * <p>Possible Values:</p>
     * <ul>
     *     <li>HORIZON (default): Start reading from the oldest available message</li>
     *     <li>LATEST: Start reading from the latest message</li>
     * </ul>
     *
     * @return LATEST or HORIZON
     */
    String startFrom() default "HORIZON";
}

