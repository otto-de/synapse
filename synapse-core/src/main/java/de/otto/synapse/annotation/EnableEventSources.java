package de.otto.synapse.annotation;

import de.otto.synapse.configuration.EventSourcingAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(EventSourceBeanRegistrar.class)
@EnableEventSourcing
public @interface EnableEventSources {
    EnableEventSource[] value();
}
