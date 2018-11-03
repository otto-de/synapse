package de.otto.synapse.configuration.kinesis;

import de.otto.synapse.testsupport.LocalS3Client;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;


@Configuration
public class S3TestConfiguration {

    private static final String INTEGRATION_TEST_SNAPSHOT_BUCKET = "de-otto-integrationtest-snapshots";

    @Bean
    public S3Client fakeS3Client() {
        final LocalS3Client localS3Client = new LocalS3Client();
        localS3Client.createBucket(CreateBucketRequest.builder().bucket(INTEGRATION_TEST_SNAPSHOT_BUCKET).build());
        return localS3Client;
    }
}
