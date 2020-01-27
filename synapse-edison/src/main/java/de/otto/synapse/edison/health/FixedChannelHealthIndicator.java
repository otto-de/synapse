package de.otto.synapse.edison.health;

import de.otto.synapse.eventsource.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * A Spring Boot HealthIndicator that is healthy after finishing all events of a fixed list of
 * {@link EventSource} for the first time.
 */
@Component
@ConditionalOnProperty(
        prefix = "synapse.edison.health",
        name = "fixedChannel.enabled",
        havingValue = "true",
        matchIfMissing = true)
@ConditionalOnBean(EventSource.class)
public class FixedChannelHealthIndicator extends AbstractChannelHealthIndicator {

    @Autowired
    public FixedChannelHealthIndicator(@Value("${synapse.edison.health.fixedChannel.names}") final Set<String> fixedChannelNames) {
        super(fixedChannelNames);
    }
}
