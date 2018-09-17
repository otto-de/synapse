package de.otto.synapse.configuration.aws;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;

import java.net.URI;

@Configuration
public class SqsTestConfiguration {

    @Bean
    public SQSAsyncClient sqsAsyncClient() {
        return SQSAsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsCredentials.create("foobar", "foobar")))
                .endpointOverride(URI.create("http://localhost:4576"))
                .build();
    }

}
