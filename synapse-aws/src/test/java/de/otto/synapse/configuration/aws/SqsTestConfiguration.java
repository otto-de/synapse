package de.otto.synapse.configuration.aws;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.net.URI;

@Configuration
public class SqsTestConfiguration {

    @Bean
    public SqsAsyncClient SqsAsyncClient() {
        return SqsAsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("foobar", "foobar")))
                .endpointOverride(URI.create("http://localhost:4576"))
                .build();
    }

}
