package de.otto.synapse.edison.health;

import com.google.common.collect.ImmutableMap;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import static org.springframework.boot.actuate.health.Health.*;

/**
 * A Spring Boot HealthIndicator that is healthy after finishing the first (snapshot)
 * {@link de.otto.synapse.eventsource.EventSource}
 */
@Component
public class StartupHealthIndicator implements HealthIndicator {

    private ChannelInfoProvider provider;

    public StartupHealthIndicator(final ChannelInfoProvider provider) {
        this.provider = provider;
    }

    @Override
    public Health health() {
        final Builder builder;

        if (provider.getInfos().isAtHead()) {
            builder = up();
        } else {
            builder = down();
        }
        provider.getInfos().getChannels().forEach(channel -> {
            ChannelInfo startupInfo = provider.getInfos().getStartupInfo(channel);
            builder.withDetail(channel, ImmutableMap.of(
                    "status", startupInfo.getStatus().name(),
                    "message", startupInfo.getMessage())
            );
        });
        return builder.build();
    }
}
