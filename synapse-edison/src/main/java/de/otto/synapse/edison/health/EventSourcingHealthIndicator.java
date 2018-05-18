package de.otto.synapse.edison.health;

import de.otto.synapse.eventsource.EventSourceNotification;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        prefix = "synapse",
        name = "consumer-process.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class EventSourcingHealthIndicator implements HealthIndicator {

    private volatile Health health = Health.up().build();

    @EventListener
    public void onEventSourceNotification(final EventSourceNotification eventSourceNotification) {
        if (eventSourceNotification.getStatus() == EventSourceNotification.Status.FAILED) {
            health = Health.down()
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
