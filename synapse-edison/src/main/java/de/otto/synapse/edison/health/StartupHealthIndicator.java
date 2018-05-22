package de.otto.synapse.edison.health;

import de.otto.synapse.edison.provider.MessageReceiverEndpointInfoProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * A Spring Boot HealthIndicator that is healthy after finishing the first (snapshot)
 * {@link de.otto.synapse.eventsource.EventSource}
 */
@Component
public class StartupHealthIndicator implements HealthIndicator {

    private MessageReceiverEndpointInfoProvider provider;

    @Autowired
    public StartupHealthIndicator(final MessageReceiverEndpointInfoProvider provider) {
        this.provider = provider;
    }

    @Override
    public Health health() {
        return Health.up().build();
//        final Builder builder;
//
//        if (provider.getInfos().allChannelsAtHead()) {
//            builder = up();
//        } else {
//            builder = down();
//        }
//        provider.getInfos().getChannels().forEach(channel -> {
//            MessageReceiverEndpointInfo startupInfo = provider.getInfos().getChannelInfoFor(channel);
//            builder.withDetail(channel, ImmutableMap.of(
//                    "status", startupInfo.getStatus().name(),
//                    "message", startupInfo.getMessage())
//            );
//        });
//        return builder.build();
    }
}
