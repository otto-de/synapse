package de.otto.synapse.annotation.messagequeue;

import de.otto.synapse.configuration.SynapseAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({
        SynapseAutoConfiguration.class,
        MessageQueueSenderEndpointBeanRegistrar.class})
public @interface EnableMessageSenderEndpoints {
    EnableMessageSenderEndpoint[] value();
}
