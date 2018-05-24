package de.otto.synapse.edison.health;

import de.otto.synapse.edison.provider.MessageReceiverEndpointInfoProvider;
import de.otto.synapse.info.MessageEndpointStatus;
import de.otto.synapse.info.MessageReceiverEndpointInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.springframework.boot.actuate.health.Health.*;

/**
 * A Spring Boot HealthIndicator that is healthy after finishing the first (snapshot)
 * {@link de.otto.synapse.eventsource.EventSource}
 */
@Component
@ConditionalOnProperty(
        prefix = "synapse",
        name = "health-indicator.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class StartupHealthIndicator implements HealthIndicator {

    private MessageReceiverEndpointInfoProvider provider;

    @Autowired
    public StartupHealthIndicator(final MessageReceiverEndpointInfoProvider provider) {
        this.provider = provider;
    }

    @Override
    public Health health() {
        final Builder healthBuilder;
        if (provider.allChannelsStarted()) {
            healthBuilder = up().withDetail("message", "All channels up to date");
        } else {
            healthBuilder = down().withDetail("message", "Channels not yet up to date");
        }
        return healthBuilder.build();
    }

}
