package de.otto.synapse.edison.health;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.eventsource.EventSourceNotification;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static de.otto.synapse.eventsource.EventSourceNotification.Status.FINISHED;
import static org.springframework.boot.actuate.health.Health.*;

/**
 * A Spring Boot HealthIndicator that is healthy after finishing the first (snapshot)
 * {@link de.otto.synapse.eventsource.EventSource}
 */
@Component
@EnableConfigurationProperties(HealthProperties.class)
public class StartupHealthIndicator implements HealthIndicator {

    private volatile Health health = down().build();
    private SnapshotStatusProvider provider;

    public StartupHealthIndicator(final SnapshotStatusProvider provider) {
        this.provider = provider;
        updateStatus();
    }

    @EventListener
    public void onEventSourceNotification(final EventSourceNotification eventSourceNotification) {
        provider.onEventSourceNotification(eventSourceNotification);
        updateStatus();
    }

    @Override
    public Health health() {
        return health;
    }

    private void updateStatus() {
        final Builder builder;

        if (!provider.finished()) {
            builder = down();
        } else {
            builder = up();
        }
        final Map<String, Map<String, String>> details = provider.getDetails();
        details.keySet().forEach(key -> {
            builder.withDetail(key, details.get(key));
        });
        health = builder.build();
    }
}
