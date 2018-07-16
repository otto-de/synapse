package de.otto.synapse.configuration.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.configuration.AwsProperties;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpointFactory;
import de.otto.synapse.endpoint.receiver.aws.SqsMessageQueueReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.aws.SqsMessageSenderEndpointFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;

@Configuration
@EnableConfigurationProperties(AwsProperties.class)
public class SqsAutoConfiguration {

    private final AwsProperties awsProperties;

    @Autowired
    public SqsAutoConfiguration(final AwsProperties awsProperties) {
        this.awsProperties = awsProperties;
    }

    @Bean
    @ConditionalOnMissingBean(SQSAsyncClient.class)
    public SQSAsyncClient sqsAsyncClient(final AwsCredentialsProvider credentialsProvider) {
        return SQSAsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(awsProperties.getRegion()))
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public SqsMessageSenderEndpointFactory sqsSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                                                 final ObjectMapper objectMapper,
                                                                 final SQSAsyncClient sqsAsyncClient) {
        return new SqsMessageSenderEndpointFactory(registry, objectMapper, sqsAsyncClient);
    }

    // TODO Should return SqsMessageQueueReceiverEndpoint, @ConditionalOnMissingBean checks for existing MessageQueueReceiverEndpointFactory and skips SQS when Kinesis already registerd
    @Bean
    @ConditionalOnMissingBean
    public MessageQueueReceiverEndpointFactory sqsReceiverEndpointFactory(final MessageInterceptorRegistry registry,
                                                                          final ObjectMapper objectMapper,
                                                                          final SQSAsyncClient sqsAsyncClient,
                                                                          final ApplicationEventPublisher eventPublisher) {

        return (String channelName) -> {
            final SqsMessageQueueReceiverEndpoint endpoint = new SqsMessageQueueReceiverEndpoint(channelName, sqsAsyncClient, objectMapper, eventPublisher);
            endpoint.registerInterceptorsFrom(registry);
            return endpoint;
        };
    }

}
