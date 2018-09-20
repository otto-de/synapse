package de.otto.synapse.annotation;

import de.otto.synapse.configuration.EventSourcingAutoConfiguration;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
