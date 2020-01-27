package de.otto.synapse.edison.health;

import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.event.EventListener;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.springframework.boot.actuate.health.Health.down;
import static org.springframework.boot.actuate.health.Health.up;

public abstract class AbstractChannelHealthIndicator implements HealthIndicator {
    public static final long TEN_SECONDS = 10000L;
    private Set<String> allChannels;
    private Set<String> healthyChannels;

    protected AbstractChannelHealthIndicator(Set<String> allChannels) {
        this.allChannels = allChannels;
        this.healthyChannels = ConcurrentHashMap.newKeySet();;
    }


    @EventListener
    public void on(final MessageReceiverNotification notification) {
        if (notification.getStatus() == MessageReceiverStatus.RUNNING) {
            notification.getChannelDurationBehind().ifPresent(channelDurationBehind -> {
                if (channelDurationBehind.getDurationBehind().toMillis() <= TEN_SECONDS) {
                    healthyChannels.add(notification.getChannelName());
                }
            });
        }
    }

    @Override
    public Health health() {
        final Health.Builder healthBuilder;
        if (healthyChannels.containsAll(allChannels)) {
            healthBuilder = up().withDetail("message", "All channels up to date");
        } else {
            healthBuilder = down().withDetail("message", "Channel(s) not yet up to date");
        }
        return healthBuilder.build();
    }
}
