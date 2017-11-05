package de.otto.edison.eventsourcing.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.configuration.AwsConfiguration;
import de.otto.edison.aws.s3.S3Service;
import de.otto.edison.aws.s3.configuration.S3Configuration;
import de.otto.edison.eventsourcing.EventSourcingProperties;
import de.otto.edison.eventsourcing.s3.SnapshotService;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(EventSourcingProperties.class)
@ImportAutoConfiguration({
        AwsConfiguration.class,
        S3Configuration.class,
        KinesisConfiguration.class,
        EventSourcingBootstrapConfiguration.class
})
public class EventSourcingConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    @ConditionalOnMissingBean
    public SnapshotService snapshotService(final S3Service s3Service,
                                           final ObjectMapper objectMapper,
                                           final EventSourcingProperties eventSourcingProperties) {
        return new SnapshotService(s3Service, eventSourcingProperties, objectMapper);
    }
}
