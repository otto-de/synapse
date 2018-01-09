package de.otto.edison.eventsourcing.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import de.otto.edison.eventsourcing.CompactingKinesisEventSourceBuilder;
import de.otto.edison.eventsourcing.KinesisEventSourceBuilder;
import de.otto.edison.eventsourcing.SnapshotEventSourceBuilder;
import de.otto.edison.eventsourcing.consumer.EventSourceConsumerProcess;
import de.otto.edison.eventsourcing.s3.SnapshotConsumerService;
import de.otto.edison.eventsourcing.s3.SnapshotReadService;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
    @ConditionalOnMissingBean(ObjectMapper.class)
    public ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "edison.eventsourcing",
            name = "consumer-process.enabled",
            havingValue = "true",
            matchIfMissing = true)
    public EventSourceConsumerProcess eventSourceConsumerProcess() {
        return new EventSourceConsumerProcess();
    }

    @Bean
    @ConditionalOnMissingBean(name = "streamingEventSourceBuilder")
    public KinesisEventSourceBuilder streamingEventSourceBuilder(final KinesisClient kinesisClient,
                                                                 final TextEncryptor textEncryptor,
                                                                 final ObjectMapper objectMapper) {
        return new KinesisEventSourceBuilder(objectMapper, kinesisClient, textEncryptor);
    }

    @Bean
    @ConditionalOnMissingBean(name = "snapshotEventSourceBuilder")
    public SnapshotEventSourceBuilder snapshotEventSourceBuilder(final SnapshotReadService snapshotReadService,
                                                                 final SnapshotConsumerService snapshotConsumerService,
                                                                 final ObjectMapper objectMapper,
                                                                 final ApplicationEventPublisher applicationEventPublisher) {
        return new SnapshotEventSourceBuilder(snapshotReadService, snapshotConsumerService, objectMapper, applicationEventPublisher);
    }
    @Bean
    @ConditionalOnMissingBean(name = "defaultEventSourceBuilder")
    public CompactingKinesisEventSourceBuilder defaultEventSourceBuilder(final KinesisEventSourceBuilder kinesisEventSourceBuilder,
                                                                         final SnapshotEventSourceBuilder snapshotEventSourceBuilder) {
        return new CompactingKinesisEventSourceBuilder(kinesisEventSourceBuilder, snapshotEventSourceBuilder);
    }

}

