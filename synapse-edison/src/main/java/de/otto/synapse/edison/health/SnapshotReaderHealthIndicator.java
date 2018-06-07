package de.otto.synapse.edison.health;

import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.SnapshotReaderNotification;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import static de.otto.synapse.info.SnapshotReaderStatus.FAILED;

@Component
@ConditionalOnProperty(
        prefix = "synapse.edison.health",
        name = "snapshotreader.enabled",
        havingValue = "true",
        matchIfMissing = true)
@ConditionalOnBean(EventSource.class)
public class SnapshotReaderHealthIndicator implements HealthIndicator {

    private volatile Health health = Health.up().build();

    @EventListener
    public void on(final SnapshotReaderNotification notification) {
        if (notification.getStatus() == FAILED) {
            health = Health.down()
                    .withDetail("channelName", notification.getChannelName())
                    .withDetail("message", notification.getMessage())
                    .build();
        }
    }

    @Override
    public Health health() {
        return health;
    }
}
