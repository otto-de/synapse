package de.otto.edison.eventsourcing.aws.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.configuration.AwsConfiguration;
import de.otto.edison.aws.s3.configuration.S3Configuration;
import de.otto.edison.eventsourcing.aws.CompactingKinesisEventSourceBuilder;
import de.otto.edison.eventsourcing.aws.KinesisEventSourceBuilder;
import de.otto.edison.eventsourcing.aws.SnapshotEventSourceBuilder;
import de.otto.edison.eventsourcing.aws.s3.SnapshotConsumerService;
import de.otto.edison.eventsourcing.aws.s3.SnapshotReadService;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@Configuration
@ImportAutoConfiguration({
        AwsConfiguration.class,
        S3Configuration.class,
        KinesisConfiguration.class,
        SnapshotAutoConfiguration.class
})
@EnableConfigurationProperties(SnapshotProperties.class)
public class AwsEventSourcingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "streamingEventSourceBuilder")
    public KinesisEventSourceBuilder streamingEventSourceBuilder(final KinesisClient kinesisClient,
                                                                 final ApplicationEventPublisher eventPublisher,
                                                                 final ObjectMapper objectMapper) {
        return new KinesisEventSourceBuilder(objectMapper, eventPublisher, kinesisClient);
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

