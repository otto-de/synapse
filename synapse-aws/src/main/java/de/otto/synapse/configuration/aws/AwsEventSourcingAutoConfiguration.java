package de.otto.synapse.configuration.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.configuration.AwsConfiguration;
import de.otto.edison.aws.s3.configuration.S3Configuration;
import de.otto.synapse.aws.s3.SnapshotConsumerService;
import de.otto.synapse.aws.s3.SnapshotReadService;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.eventsource.aws.CompactedKinesisEventSourceBuilder;
import de.otto.synapse.eventsource.aws.KinesisEventSourceBuilder;
import de.otto.synapse.eventsource.aws.SnapshotEventSourceBuilder;
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
    @ConditionalOnMissingBean(name = "kinesisEventSourceBuilder")
    public KinesisEventSourceBuilder kinesisEventSourceBuilder(final KinesisClient kinesisClient,
                                                               final ApplicationEventPublisher eventPublisher,
                                                               final ObjectMapper objectMapper,
                                                               final MessageInterceptorRegistry registry) {
        return new KinesisEventSourceBuilder(objectMapper, eventPublisher, kinesisClient, registry);
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
    public CompactedKinesisEventSourceBuilder defaultEventSourceBuilder(final KinesisEventSourceBuilder kinesisEventSourceBuilder,
                                                                        final SnapshotEventSourceBuilder snapshotEventSourceBuilder) {
        return new CompactedKinesisEventSourceBuilder(kinesisEventSourceBuilder, snapshotEventSourceBuilder);
    }

}

