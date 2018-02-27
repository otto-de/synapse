package de.otto.synapse.edison.health;

import de.otto.synapse.consumer.EventSourceNotification;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class EventSourcingHealthIndicator implements HealthIndicator {

    private Health health = Health.up().build();

    @EventListener
    public void onEventSourceNotification(EventSourceNotification eventSourceNotification) {
        if (eventSourceNotification.getStatus() == EventSourceNotification.Status.FAILED) {
            health = Health.down()
                    .withDetail("message", eventSourceNotification.getMessage())
                    .withDetail("stream", eventSourceNotification.getStreamName())
                    .build();
        }
    }

    @Override
    public Health health() {
        return health;
    }
}
