package de.otto.synapse.configuration.aws;

import de.otto.edison.aws.configuration.AwsConfiguration;
import de.otto.edison.aws.s3.configuration.S3Configuration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ImportAutoConfiguration({
        AwsConfiguration.class,
        S3Configuration.class,
        KinesisAutoConfiguration.class,
        SnapshotAutoConfiguration.class
})
@EnableConfigurationProperties(SnapshotProperties.class)
public class AwsEventSourcingAutoConfiguration {

}
