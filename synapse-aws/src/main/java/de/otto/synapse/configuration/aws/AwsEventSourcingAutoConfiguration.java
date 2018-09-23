package de.otto.synapse.configuration.aws;

import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ImportAutoConfiguration({
        AwsConfiguration.class,
        S3AutoConfiguration.class,
        KinesisAutoConfiguration.class,
        S3SnapshotAutoConfiguration.class
})
@EnableConfigurationProperties(SnapshotProperties.class)
public class AwsEventSourcingAutoConfiguration {

}
