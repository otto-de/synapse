package de.otto.edison.eventsourcing.configuration;

import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceConsumerProcess;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;

@Configuration
@ImportAutoConfiguration({
        EventSourcingBootstrapConfiguration.class,
        SnapshotConfiguration.class,
        KinesisConfiguration.class,
})
@EnableConfigurationProperties(EventSourcingProperties.class)
@ConditionalOnProperty(
        prefix = "edison.eventsourcing",
        name = "consumer-process.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class EventSourcingConfiguration {

    @Bean
    public EventSourceConsumerProcess eventSourceConsumerProcess(
            Optional<List<EventConsumer>> eventConsumers,
            Optional<List<EventSource>> eventSources) {
        return new EventSourceConsumerProcess(
                eventSources.orElse(emptyList()),
                eventConsumers.orElse(emptyList())
        );
    }
}

