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

import static org.springframework.boot.actuate.health.Health.*;

/**
 * A Spring Boot HealthIndicator that is healthy after finishing the first (snapshot)
 * {@link de.otto.synapse.eventsource.EventSource}
 */
@Component
@EnableConfigurationProperties(HealthProperties.class)
public class SnapshotStatusProvider {

    private static final String KEY_STATUS = "status";
    private static final String KEY_MESSAGE = "message";

    private static final String FINISHED = "FINISHED";
    private static final String NOT_FINISHED = "NOT FINISHED";
    private static final String CHANNEL_NOT_YET_FINISHED = "Channel not yet finished";

    private Map<String, Map<String, String>> details = new ConcurrentHashMap<>();

    public SnapshotStatusProvider(final HealthProperties properties) {
        properties.getChannels().forEach(channel -> {
            details.put(channel, ImmutableMap.of(KEY_MESSAGE, CHANNEL_NOT_YET_FINISHED, KEY_STATUS, NOT_FINISHED));
        });
    }

    @EventListener
    public void onEventSourceNotification(final EventSourceNotification eventSourceNotification) {
        if (eventSourceNotification.getStatus() == EventSourceNotification.Status.FINISHED) {
            details.put(eventSourceNotification.getChannelName(), ImmutableMap.of(KEY_MESSAGE, eventSourceNotification.getMessage(), KEY_STATUS, FINISHED));
        }
    }

    public boolean finished() {
        return !details.values().stream().anyMatch(map -> map.get(KEY_STATUS).equals(NOT_FINISHED));
    }

    public Map<String, Map<String, String>> getDetails() {
        return details;
    }

}
