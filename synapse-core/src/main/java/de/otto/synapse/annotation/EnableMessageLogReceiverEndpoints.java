package de.otto.synapse.annotation;

import de.otto.synapse.configuration.MessageLogReceiverEndpointAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({
        MessageLogReceiverEndpointAutoConfiguration.class,
        MessageLogReceiverEndpointBeanRegistrar.class})
public @interface EnableMessageLogReceiverEndpoints {
    EnableMessageLogReceiverEndpoint[] value();
}
