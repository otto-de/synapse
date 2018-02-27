package de.otto.synapse.configuration.aws;

import de.otto.edison.aws.s3.S3Service;
import de.otto.synapse.aws.s3.SnapshotConsumerService;
import de.otto.synapse.aws.s3.SnapshotReadService;
import de.otto.synapse.aws.s3.SnapshotWriteService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(SnapshotProperties.class)
public class SnapshotAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SnapshotReadService snapshotReadService(final S3Service s3Service,
                                                   final SnapshotProperties snapshotProperties) {
        return new SnapshotReadService(snapshotProperties, s3Service);
    }
    @Bean
    @ConditionalOnMissingBean
    public SnapshotWriteService snapshotWriteService(final S3Service s3Service,
                                                     final SnapshotProperties snapshotProperties) {
        return new SnapshotWriteService(s3Service, snapshotProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    public SnapshotConsumerService snapshotConsumerService() {
        return new SnapshotConsumerService();
    }

}
