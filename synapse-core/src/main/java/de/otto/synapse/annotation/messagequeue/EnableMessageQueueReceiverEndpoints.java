package de.otto.synapse.annotation.messagequeue;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(EnableMessageQueueReceiverEndpointBeanRegistrar.class)
public @interface EnableMessageQueueReceiverEndpoints {
    EnableMessageQueueReceiverEndpoint[] value();
}
