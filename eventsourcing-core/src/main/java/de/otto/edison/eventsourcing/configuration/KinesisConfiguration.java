package de.otto.edison.eventsourcing.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.configuration.AwsProperties;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSenderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.security.crypto.encrypt.TextEncryptor;
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
    public KinesisEventSenderFactory kinesisEventSenderFactory(ObjectMapper objectMapper,
                                                               TextEncryptor textEncryptor,
                                                               KinesisClient kinesisClient) {
        return new KinesisEventSenderFactory(objectMapper, textEncryptor, kinesisClient);
    }

    @Bean
    @ConditionalOnMissingBean(TextEncryptor.class)
    public TextEncryptor textEncryptor() {
        return Encryptors.noOpText();
    }

}
