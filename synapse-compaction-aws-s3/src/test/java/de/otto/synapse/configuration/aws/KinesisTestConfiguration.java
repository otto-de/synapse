package de.otto.synapse.configuration.aws;

import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.kinesis.KinesisMessageSender;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.TextMessageTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.net.URI;

import static de.otto.synapse.testsupport.KinesisChannelSetupUtils.createChannelIfNotExists;

public class KinesisTestConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisTestConfiguration.class);

    private static final String INTEGRATION_TEST_STREAM = "kinesis-compaction-test";

    @Bean
    public AwsCredentialsProvider awsCredentialsProvider() {
        LOG.info("Configuring StaticCredentialsProvider for local tests");
        return StaticCredentialsProvider.create(AwsBasicCredentials.create("foobar", "foobar"));
    }

    @Bean
    @Primary
    public KinesisAsyncClient kinesisAsyncClient(final @Value("${test.environment:local}") String testEnvironment,
                                                 final AwsCredentialsProvider credentialsProvider) {
        // kinesalite does not support cbor at the moment (v1.11.6)
        System.setProperty("aws.cborEnabled", "false");
        LOG.info("kinesis client for local tests");
        final KinesisAsyncClient kinesisClient;
        if (testEnvironment.equals("local")) {
            kinesisClient = KinesisAsyncClient.builder()
                    .httpClient(
                            // Disables HTTP2 because of problems with LocalStack
                            NettyNioAsyncHttpClient.builder()
                                    .protocol(Protocol.HTTP1_1)
                                    .build())
                    .endpointOverride(URI.create("http://localhost:4566"))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(credentialsProvider)
                    .build();
        } else {
            kinesisClient = KinesisAsyncClient.builder()
                    .credentialsProvider(credentialsProvider)
                    .build();
        }
        createChannelIfNotExists(kinesisClient, INTEGRATION_TEST_STREAM, 2);
        return kinesisClient;
    }

    @Bean
    public MessageSenderEndpoint kinesisV1Sender(final MessageInterceptorRegistry registry,
                                                 final KinesisAsyncClient kinesisClient) {
        return new KinesisMessageSender(INTEGRATION_TEST_STREAM, registry, new TextMessageTranslator(), kinesisClient, MessageFormat.V1);
    }

    @Bean
    public MessageSenderEndpoint kinesisV2Sender(final MessageInterceptorRegistry registry,
                                                 final KinesisAsyncClient kinesisClient) {
        return new KinesisMessageSender(INTEGRATION_TEST_STREAM, registry, new TextMessageTranslator(), kinesisClient, MessageFormat.V2);
    }

}
