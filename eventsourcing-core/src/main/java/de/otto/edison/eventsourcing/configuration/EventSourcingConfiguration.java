package de.otto.edison.eventsourcing.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSourceFactory;
import de.otto.edison.eventsourcing.annotation.EventSourceMapping;
import de.otto.edison.eventsourcing.consumer.EventSourceConsumerProcess;
import de.otto.edison.eventsourcing.s3.SnapshotConsumerService;
import de.otto.edison.eventsourcing.s3.SnapshotReadService;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import software.amazon.awssdk.services.kinesis.KinesisClient;

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
            EventSourceMapping mapping,
            ObjectMapper objectMapper) {
        return new EventSourceConsumerProcess(
                mapping,
                objectMapper
        );
    }

    @Bean
    public EventSourceMapping eventSourceMapping() {
        return new EventSourceMapping();
    }

    @Bean
    public EventSourceFactory eventSourceFactory(
            SnapshotReadService snapshotReadService,
            SnapshotConsumerService snapshotConsumerService,
            ObjectMapper objectMapper,
            KinesisClient kinesisClient,
            TextEncryptor textEncryptor,
            ApplicationEventPublisher applicationEventPublisher) {
        return new EventSourceFactory(
                snapshotReadService,
                snapshotConsumerService,
                objectMapper,
                kinesisClient,
                textEncryptor,
                applicationEventPublisher);
    }
}

