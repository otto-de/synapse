package de.otto.synapse.configuration.aws;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.core.auth.AwsCredentials;
import software.amazon.awssdk.core.auth.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;

import java.net.URI;

@Configuration
public class SqsTestConfiguration {

    @Bean
    public SQSAsyncClient sqsAsyncClient() {
        return SQSAsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsCredentials.create("foobar", "foobar")))
                .endpointOverride(URI.create("http://localhost:4576"))
                .build();
    }

}
