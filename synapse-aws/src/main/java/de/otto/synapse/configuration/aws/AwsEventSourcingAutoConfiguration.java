package de.otto.synapse.configuration.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.configuration.AwsConfiguration;
import de.otto.edison.aws.s3.configuration.S3Configuration;
import de.otto.synapse.channel.aws.KinesisMessageLogReceiverEndpoint;
import de.otto.synapse.compaction.aws.SnapshotReadService;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.DefaultEventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.aws.CompactedKinesisEventSourceBuilder;
import de.otto.synapse.eventsource.aws.KinesisEventSourceBuilder;
import de.otto.synapse.eventsource.aws.SnapshotEventSourceBuilder;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.aws.SnapshotMessageStore;
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
                                                                 final ObjectMapper objectMapper,
                                                                 final ApplicationEventPublisher applicationEventPublisher) {
        return new SnapshotEventSourceBuilder(snapshotReadService, objectMapper, applicationEventPublisher);
    }

    @Bean
    @ConditionalOnMissingBean(name = "defaultEventSourceBuilder")
    public CompactedKinesisEventSourceBuilder defaultEventSourceBuilder(final KinesisEventSourceBuilder kinesisEventSourceBuilder,
                                                                        final SnapshotEventSourceBuilder snapshotEventSourceBuilder) {
        return new CompactedKinesisEventSourceBuilder(kinesisEventSourceBuilder, snapshotEventSourceBuilder);
    }

    @Bean
    @ConditionalOnMissingBean(name = "newDefaultEventSourceBuilder")
    public EventSourceBuilder newDefaultEventSourceBuilder(final SnapshotReadService snapshotReadService,
                                                           final MessageInterceptorRegistry interceptorRegistry,
                                                           final KinesisClient kinesisClient,
                                                           final ApplicationEventPublisher eventPublisher,
                                                           final ObjectMapper objectMapper) {
        return (name, channelName) -> {
            final MessageStore messageStore = new SnapshotMessageStore(channelName, snapshotReadService);
            final MessageLogReceiverEndpoint messageLog = new KinesisMessageLogReceiverEndpoint(channelName, kinesisClient, objectMapper, eventPublisher);
            messageLog.registerInterceptorsFrom(interceptorRegistry);
            return new DefaultEventSource(channelName, messageStore, messageLog);
        };
    }

}
