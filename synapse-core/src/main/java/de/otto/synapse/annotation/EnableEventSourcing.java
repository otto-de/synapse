package de.otto.synapse.annotation;

import de.otto.synapse.configuration.EventSourcingAutoConfiguration;
import de.otto.synapse.eventsource.EventSource;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Enables auto-configuration of {@link EventSource event sources} for applications that do not use
 * annotation-based configuration of event sources using {@link EnableEventSource}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(EventSourcingAutoConfiguration.class)
public @interface EnableEventSourcing {
}
