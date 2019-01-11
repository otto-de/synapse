package de.otto.synapse.edison.health;

import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
import io.netty.util.internal.ConcurrentSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static org.springframework.boot.actuate.health.Health.*;

/**
 * A Spring Boot HealthIndicator that is healthy after finishing all events of an
 * {@link de.otto.synapse.eventsource.EventSource} for the first time.
 */
@Component
@ConditionalOnProperty(
        prefix = "synapse.edison.health",
        name = "startup.enabled",
        havingValue = "true",
        matchIfMissing = true)
@ConditionalOnBean(EventSource.class)
public class StartupHealthIndicator implements HealthIndicator {

    public static final long TEN_SECONDS = 10000L;
    private Set<String> allChannels;
    private Set<String> healthyChannels;

    @Autowired
    public StartupHealthIndicator(final Optional<List<EventSource>> eventSources) {
        allChannels = eventSources.orElse(emptyList())
                .stream()
                .map(EventSource::getChannelName)
                .collect(toSet());
        healthyChannels = new ConcurrentSet<>();
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
        final Builder healthBuilder;
        if (healthyChannels.containsAll(allChannels)) {
            healthBuilder = up().withDetail("message", "All channels up to date");
        } else {
            healthBuilder = down().withDetail("message", "Channel(s) not yet up to date");
        }
        return healthBuilder.build();
    }

}
