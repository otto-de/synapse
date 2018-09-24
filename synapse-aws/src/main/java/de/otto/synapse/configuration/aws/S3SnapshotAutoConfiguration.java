package de.otto.synapse.configuration.aws;

import de.otto.synapse.compaction.aws.SnapshotReadService;
import de.otto.synapse.compaction.aws.SnapshotWriteService;
import de.otto.synapse.messagestore.MessageStoreFactory;
import de.otto.synapse.messagestore.SnapshotMessageStore;
import de.otto.synapse.messagestore.aws.S3SnapshotMessageStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
@EnableConfigurationProperties(SnapshotProperties.class)
@Import(S3AutoConfiguration.class)
public class S3SnapshotAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SnapshotReadService snapshotReadService(final S3Client s3Client,
                                                   final SnapshotProperties snapshotProperties) {
        return new SnapshotReadService(snapshotProperties, s3Client);
    }
    @Bean
    @ConditionalOnMissingBean
    public SnapshotWriteService snapshotWriteService(final S3Client s3Client,
                                                     final SnapshotProperties snapshotProperties) {
        return new SnapshotWriteService(s3Client, snapshotProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    MessageStoreFactory<SnapshotMessageStore> snapshotMessageStoreFactory(final SnapshotReadService snapshotReadService,
                                                                          final ApplicationEventPublisher eventPublisher) {
        return (channelName) -> new S3SnapshotMessageStore(channelName, snapshotReadService, eventPublisher);
    }

}
