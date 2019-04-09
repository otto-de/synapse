package de.otto.synapse.configuration.sqs;

import de.otto.synapse.endpoint.SqsClientHelper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.net.URI;

@Configuration
public class SqsTestConfiguration {

    public static final String SQS_INTEGRATION_TEST_CHANNEL = "sqs-test-channel";

    @Bean
    public SqsAsyncClient sqsAsyncClient() {
        final SqsAsyncClient sqsAsyncClient = SqsAsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("foobar", "foobar")))
                .endpointOverride(URI.create("http://localhost:4576"))
                .build();
        new SqsClientHelper(sqsAsyncClient).createChannelIfNotExists(SQS_INTEGRATION_TEST_CHANNEL);
        return sqsAsyncClient;
    }

}
