package de.otto.synapse.configuration.aws;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

import static software.amazon.awssdk.regions.Region.of;

@Configuration
@EnableConfigurationProperties(AwsProperties.class)
public class S3AutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public S3Client s3Client(final AwsProperties awsProperties,
                             final AwsCredentialsProvider awsCredentialsProvider) {
        return S3Client
                .builder()
                .region(of(awsProperties.getRegion()))
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

}
