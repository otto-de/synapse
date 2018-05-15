package de.otto.synapse.configuration.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.configuration.AwsProperties;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderFactory;
import de.otto.synapse.endpoint.sender.aws.KinesisMessageSenderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.core.auth.AwsCredentialsProvider;
import software.amazon.awssdk.core.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@Configuration
@EnableConfigurationProperties(AwsProperties.class)
public class KinesisConfiguration {

    private final AwsProperties awsProperties;

    @Autowired
    public KinesisConfiguration(final AwsProperties awsProperties) {
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
    @ConditionalOnMissingBean(MessageSenderFactory.class)
    public MessageSenderFactory messageSenderFactory(final MessageInterceptorRegistry registry,
                                                     final ObjectMapper objectMapper,
                                                     final KinesisClient kinesisClient) {
        return new KinesisMessageSenderFactory(registry, objectMapper, kinesisClient);
    }

}
