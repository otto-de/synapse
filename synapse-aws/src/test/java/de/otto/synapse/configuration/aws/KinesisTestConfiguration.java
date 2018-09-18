package de.otto.synapse.configuration.aws;

import de.otto.synapse.configuration.MessageEndpointConfigurer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.builder.ClientHttpConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpClientFactory;
import software.amazon.awssdk.http.apache.ApacheSdkHttpClientFactory;
import software.amazon.awssdk.http.nio.netty.NettySdkHttpClientFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.net.URI;
import java.time.Duration;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.receiverChannelsWith;
import static org.slf4j.LoggerFactory.getLogger;
import static software.amazon.awssdk.services.kinesis.KinesisClient.builder;

@Configuration
public class KinesisTestConfiguration implements MessageEndpointConfigurer {

    private static final Logger LOG = getLogger(KinesisTestConfiguration.class);

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
        System.setProperty("aws.cborEnabled", "false");
        LOG.info("kinesis client for local tests");
        if (testEnvironment.equals("local")) {
            final SdkHttpClientFactory factory = ApacheSdkHttpClientFactory.builder()
                    .connectionTimeout(Duration.ofSeconds(10))
                    .socketTimeout(Duration.ofSeconds(10))
                    .build();
            return builder()
                    .httpConfiguration(ClientHttpConfiguration.builder().httpClientFactory(factory).build())
                    .endpointOverride(URI.create("http://localhost:4568"))
                    .region(Region.EU_CENTRAL_1)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsCredentials.create("foobar", "foobar")))
                    .build();
        } else {
            return builder()
                    .credentialsProvider(credentialsProvider)
                    .build();
        }
    }

}