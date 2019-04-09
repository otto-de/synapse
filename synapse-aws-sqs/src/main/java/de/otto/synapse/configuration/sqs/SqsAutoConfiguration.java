package de.otto.synapse.configuration.sqs;

import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.configuration.aws.AwsProperties;
import de.otto.synapse.configuration.aws.SynapseAwsAuthConfiguration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpointFactory;
import de.otto.synapse.endpoint.receiver.sqs.SqsMessageQueueReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.sqs.SqsMessageSenderEndpointFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.time.Duration;

import static software.amazon.awssdk.core.retry.conditions.RetryCondition.defaultRetryCondition;

@Configuration
@Import({SynapseAwsAuthConfiguration.class, SynapseAutoConfiguration.class})
@EnableConfigurationProperties(AwsProperties.class)
public class SqsAutoConfiguration {

    private final AwsProperties awsProperties;

    @Autowired
    public SqsAutoConfiguration(final AwsProperties awsProperties) {
        this.awsProperties = awsProperties;
    }

    @Bean
    @ConditionalOnMissingBean(SqsAsyncClient.class)
    public SqsAsyncClient sqsAsyncClient(final AwsCredentialsProvider credentialsProvider) {
        return SqsAsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(awsProperties.getRegion()))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallAttemptTimeout(Duration.ofSeconds(5))
                        .retryPolicy(sqsRetryPolicy()).build())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "sqsRetryPolicy", value = RetryPolicy.class)
    public RetryPolicy sqsRetryPolicy() {
        return RetryPolicy.defaultRetryPolicy().toBuilder()
                .retryCondition(defaultRetryCondition())
                .numRetries(Integer.MAX_VALUE)
                .backoffStrategy(FullJitterBackoffStrategy.builder()
                        .baseDelay(Duration.ofSeconds(1))
                        .maxBackoffTime(SdkDefaultRetrySetting.MAX_BACKOFF)
                        .build())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "messageQueueSenderEndpointFactory")
    public MessageSenderEndpointFactory messageQueueSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                                                          final SqsAsyncClient sqsAsyncClient,
                                                                          final @Value("${spring.application.name:Synapse Service}") String messageSenderName) {
        return new SqsMessageSenderEndpointFactory(registry, sqsAsyncClient);
    }

    @Bean
    @ConditionalOnMissingBean(name = "messageQueueReceiverEndpointFactory")
    public MessageQueueReceiverEndpointFactory messageQueueReceiverEndpointFactory(final MessageInterceptorRegistry registry,
                                                                                   final SqsAsyncClient sqsAsyncClient,
                                                                                   final ApplicationEventPublisher eventPublisher) {

        return new SqsMessageQueueReceiverEndpointFactory(registry, sqsAsyncClient, eventPublisher);
    }

}
