package de.otto.edison.eventsourcing.aws.configuration;

import de.otto.edison.eventsourcing.aws.s3.local.LocalS3Client;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.s3.S3Client;


@Configuration
public class S3TestConfiguration {

    @Bean
    public S3Client fakeS3Client() {
        return new LocalS3Client();
    }
}
