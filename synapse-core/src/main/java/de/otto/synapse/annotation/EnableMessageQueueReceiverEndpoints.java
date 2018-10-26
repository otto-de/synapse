package de.otto.synapse.annotation;

import de.otto.synapse.configuration.MessageQueueReceiverEndpointAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({
        MessageQueueReceiverEndpointAutoConfiguration.class,
        MessageQueueReceiverEndpointBeanRegistrar.class})
public @interface EnableMessageQueueReceiverEndpoints {
    EnableMessageQueueReceiverEndpoint[] value();
}
