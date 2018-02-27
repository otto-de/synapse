package de.otto.synapse.configuration.aws;

import de.otto.synapse.client.aws.LocalS3Client;
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
