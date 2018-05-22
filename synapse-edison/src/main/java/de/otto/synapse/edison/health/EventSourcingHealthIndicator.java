package de.otto.synapse.edison.health;

import de.otto.synapse.info.MessageEndpointNotification;
import de.otto.synapse.info.MessageEndpointStatus;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class EventSourcingHealthIndicator implements HealthIndicator {

    private volatile Health health = Health.up().build();

    @EventListener
    public void onEventSourceNotification(final MessageEndpointNotification messageEndpointNotification) {
        if (messageEndpointNotification.getStatus() == MessageEndpointStatus.FAILED) {
            health = Health.down()
                    .withDetail("message", messageEndpointNotification.getMessage())
                    .withDetail("channelName", messageEndpointNotification.getChannelName())
                    .build();
        }
    }

    @Override
    public Health health() {
        return health;
    }
}
