package de.otto.synapse.configuration.aws;

import de.otto.synapse.configuration.MessageEndpointConfigurer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.net.URI;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.receiverChannelsWith;
import static de.otto.synapse.testsupport.KinesisChannelSetupUtils.createChannelIfNotExists;
import static org.slf4j.LoggerFactory.getLogger;
import static software.amazon.awssdk.services.kinesis.KinesisClient.builder;

@Configuration
public class KinesisTestConfiguration implements MessageEndpointConfigurer {

    private static final Logger LOG = getLogger(KinesisTestConfiguration.class);

    public static final int EXPECTED_NUMBER_OF_SHARDS = 2;
    public static final String KINESIS_INTEGRATION_TEST_CHANNEL = "kinesis-ml-test-channel";

    @Override
    public void configureMessageInterceptors(final MessageInterceptorRegistry registry) {
        registry.register(receiverChannelsWith(testMessageInterceptor()));
    }

    @Bean
    public TestMessageInterceptor testMessageInterceptor() {
        return new TestMessageInterceptor();
    }


    @Bean
    @Primary
    public KinesisClient kinesisClient(final @Value("${test.environment:local}") String testEnvironment,
                                       final AwsCredentialsProvider credentialsProvider) {
        // kinesalite does not support cbor at the moment (v1.11.6)
        LOG.info("kinesis client for local tests");
        final KinesisClient kinesisClient;
        if (testEnvironment.equals("local")) {
            kinesisClient = builder()
                    .endpointOverride(URI.create("http://localhost:4568"))
                    .region(Region.EU_CENTRAL_1)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("foobar", "foobar")))
                    .build();
        } else {
            kinesisClient = builder()
                    .credentialsProvider(credentialsProvider)
                    .build();
        }
        createChannelIfNotExists(kinesisClient, KINESIS_INTEGRATION_TEST_CHANNEL, EXPECTED_NUMBER_OF_SHARDS);
        return kinesisClient;
    }

}
