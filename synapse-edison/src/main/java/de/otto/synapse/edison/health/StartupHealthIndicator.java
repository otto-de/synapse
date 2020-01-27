package de.otto.synapse.edison.health;

import de.otto.synapse.eventsource.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;

/**
 * A Spring Boot HealthIndicator that is healthy after finishing all events of an
 * {@link de.otto.synapse.eventsource.EventSource} for the first time.
 *
 * Caution: If you add eventsources asynchronously please use {@link FixedChannelHealthIndicator} instead.
 */
@Component
@ConditionalOnProperty(
        prefix = "synapse.edison.health",
        name = "startup.enabled",
        havingValue = "true",
        matchIfMissing = true)
@ConditionalOnBean(EventSource.class)
public class StartupHealthIndicator extends AbstractChannelHealthIndicator {

    @Autowired
    public StartupHealthIndicator(final Optional<List<EventSource>> eventSources) {
        super(eventSources.orElse(emptyList())
                .stream()
                .map(EventSource::getChannelName)
                .collect(toSet()));
    }
}
