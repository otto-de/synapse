package de.otto.synapse.edison.health;

import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
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
public class MessageReceiverEndpointHealthIndicator implements HealthIndicator {

    private volatile Health health = Health.up().build();

    @EventListener
    public void on(final MessageReceiverNotification messageEndpointNotification) {
        if (messageEndpointNotification.getStatus() == MessageReceiverStatus.FAILED) {
            health = Health.down()
                    .withDetail("channelName", messageEndpointNotification.getChannelName())
                    .withDetail("message", messageEndpointNotification.getMessage())
                    .build();
        }
    }

    @Override
    public Health health() {
        return health;
    }
}
