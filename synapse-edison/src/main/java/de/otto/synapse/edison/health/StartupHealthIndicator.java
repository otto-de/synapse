package de.otto.synapse.edison.health;

import de.otto.synapse.eventsource.EventSourceNotification;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import static de.otto.synapse.eventsource.EventSourceNotification.Status.FINISHED;
import static org.springframework.boot.actuate.health.Health.down;

/**
 * A Spring Boot HealthIndicator that is healthy after finishing the first (snapshot)
 * {@link de.otto.synapse.eventsource.EventSource}
 */
@Component
@ConditionalOnProperty(
        prefix = "synapse.edison.health",
        name = "startup.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class StartupHealthIndicator implements HealthIndicator {

    private volatile Health health = down().build();

    @EventListener
    public void onEventSourceNotification(final EventSourceNotification eventSourceNotification) {
        if (eventSourceNotification.getStatus() == FINISHED) {
            health = Health.up()
                    .withDetail("message", eventSourceNotification.getMessage())
                    .withDetail("channelName", eventSourceNotification.getChannelName())
                    .build();
        }
    }

    @Override
    public Health health() {
        return health;
    }
}
