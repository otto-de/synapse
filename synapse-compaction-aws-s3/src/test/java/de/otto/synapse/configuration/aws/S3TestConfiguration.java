package de.otto.synapse.configuration.aws;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.net.URISyntaxException;


@Configuration
public class S3TestConfiguration {

    private static final String INTEGRATION_TEST_SNAPSHOT_BUCKET = "de-otto-integrationtest-snapshots";

    @Bean
    public S3Client localStackS3Client() throws URISyntaxException {
        S3Client s3Client = S3Client.builder().endpointOverride(new URI("http://localhost:4566")).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("foobar", "foobar"))).build();
        //localS3Client.createBucket(CreateBucketRequest.builder().bucket(INTEGRATION_TEST_SNAPSHOT_BUCKET).build());
        return s3Client;
    }
}
