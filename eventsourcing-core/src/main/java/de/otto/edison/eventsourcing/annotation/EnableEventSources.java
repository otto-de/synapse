package de.otto.edison.eventsourcing.annotation;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(EnableEventSourceImportSelector.class)
public @interface EnableEventSources {
    EnableEventSource[] value();
}
