package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.configuration.EventSourceConsumerProcessConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({EventSourceBeanRegistrar.class, EventSourceConsumerProcessConfiguration.class})
public @interface EnableEventSources {
    EnableEventSource[] value();
}
