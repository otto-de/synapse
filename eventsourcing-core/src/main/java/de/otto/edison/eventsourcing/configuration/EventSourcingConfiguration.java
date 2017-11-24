package de.otto.edison.eventsourcing.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSourceFactory;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceConsumerProcess;
import de.otto.edison.eventsourcing.s3.SnapshotConsumerService;
import de.otto.edison.eventsourcing.s3.SnapshotReadService;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import software.amazon.awssdk.services.kinesis.KinesisClient;

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
public class EventSourcingConfiguration {

    @Bean
    @ConditionalOnProperty(
            prefix = "edison.eventsourcing",
            name = "consumer-process.enabled",
            havingValue = "true",
            matchIfMissing = true)
    public EventSourceConsumerProcess eventSourceConsumerProcess(
            Optional<List<EventConsumer>> eventConsumers,
            Optional<List<EventSource>> eventSources) {
        return new EventSourceConsumerProcess(
                eventSources.orElse(emptyList()),
                eventConsumers.orElse(emptyList())
        );
    }

    @Bean
    public EventSourceFactory eventSourceFactory(
            SnapshotReadService snapshotReadService,
            SnapshotConsumerService snapshotConsumerService,
            ObjectMapper objectMapper,
            KinesisClient kinesisClient,
            TextEncryptor textEncryptor)
    {
        return new EventSourceFactory(
                snapshotReadService,
                snapshotConsumerService,
                objectMapper,
                kinesisClient,
                textEncryptor);
    }
}

