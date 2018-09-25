package de.otto.synapse.configuration.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.receiver.aws.KinesisMessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.aws.KinesisMessageSenderEndpointFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@Configuration
@Import({SynapseAwsConfiguration.class, SynapseAutoConfiguration.class})
@EnableConfigurationProperties(AwsProperties.class)
public class KinesisAutoConfiguration {

    private final AwsProperties awsProperties;

    @Autowired
    public KinesisAutoConfiguration(final AwsProperties awsProperties) {
        this.awsProperties = awsProperties;
    }

    @Bean
    @ConditionalOnMissingBean(KinesisClient.class)
    public KinesisClient kinesisClient(final AwsCredentialsProvider credentialsProvider) {
        // parsing of approximateArrivalTimestamp does not work with cbor format
        // see https://github.com/aws/aws-sdk-java-v2/issues/184
        // fixed in preview-5
        System.setProperty("aws.cborEnabled", "false");
        return KinesisClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(awsProperties.getRegion()))
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "messageLogSenderEndpointFactory")
    public MessageSenderEndpointFactory messageLogSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                                                        final ObjectMapper objectMapper,
                                                                        final KinesisClient kinesisClient) {
        return new KinesisMessageSenderEndpointFactory(registry, objectMapper, kinesisClient);
    }

    @Bean
    @ConditionalOnMissingBean(name = "messageLogReceiverEndpointFactory")
    public MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                                               final ObjectMapper objectMapper,
                                                                               final KinesisClient kinesisClient,
                                                                               final ApplicationEventPublisher eventPublisher) {
        return new KinesisMessageLogReceiverEndpointFactory(interceptorRegistry, kinesisClient, objectMapper, eventPublisher);
    }

}
