package de.otto.synapse.configuration.aws;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.*;

@Configuration
@EnableConfigurationProperties(AwsProperties.class)
public class SynapseAwsConfiguration {

    @Bean
    @ConditionalOnMissingBean(AwsCredentialsProvider.class)
    public AwsCredentialsProvider awsCredentialsProvider(final AwsProperties awsProperties) {
        return AwsCredentialsProviderChain
                .builder()
                .credentialsProviders(
                        ContainerCredentialsProvider.builder().build(),
                        InstanceProfileCredentialsProvider.builder().build(),
                        EnvironmentVariableCredentialsProvider.create(),
                        ProfileCredentialsProvider
                                .builder()
                                .profileName(awsProperties.getProfile())
                                .build())
                .build();
    }

}
