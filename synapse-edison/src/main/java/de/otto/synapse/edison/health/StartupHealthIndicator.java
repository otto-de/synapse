package de.otto.synapse.edison.health;

import de.otto.synapse.edison.provider.MessageReceiverEndpointInfoProvider;
import de.otto.synapse.info.MessageEndpointStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

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

    private static final long MAX_SECONDS_BEHIND = 10L;

    private MessageReceiverEndpointInfoProvider provider;

    @Autowired
    public StartupHealthIndicator(final MessageReceiverEndpointInfoProvider provider) {
        this.provider = provider;
    }

    @Override
    public Health health() {
        final Builder healthBuilder;
        if (allChannelsUpToDate()) {
            healthBuilder = up().withDetail("message", "All channels up to date");
        } else {
            healthBuilder = down().withDetail("message", "Channels not yet up to date");
        }
        return healthBuilder.build();
    }

    private boolean allChannelsUpToDate() {
        return provider
                .getInfos()
                .stream()
                .allMatch(info -> info.getStatus() != MessageEndpointStatus.STARTING && info.getChannelPosition().get()
                        .getDurationBehind()
                        .minusSeconds(MAX_SECONDS_BEHIND)
                        .isNegative());
    }

}
