package de.otto.synapse.example.consumer.configuration;

import de.otto.synapse.annotation.messagequeue.EnableMessageQueueReceiverEndpoint;
import de.otto.synapse.configuration.InMemoryTestConfiguration;
import de.otto.synapse.configuration.MessageEndpointConfigurer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.InMemoryMessageSender;
import de.otto.synapse.endpoint.sender.InMemoryMessageSenderFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.aws.SqsMessageSender;
import de.otto.synapse.endpoint.sender.aws.SqsMessageSenderEndpointFactory;
import de.otto.synapse.example.consumer.state.BananaProduct;
import de.otto.synapse.messagequeue.InMemoryMessageQueueSender;
import de.otto.synapse.messagequeue.InMemoryMessageQueueSenderFactory;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;

import java.net.URI;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.receiverChannelsWith;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.senderChannelsWith;
import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@ImportAutoConfiguration(InMemoryTestConfiguration.class)
@EnableConfigurationProperties({MyServiceProperties.class})
@EnableMessageQueueReceiverEndpoint(name = "bananaQueue",  channelName = "${exampleservice.banana-channel}")
@EnableMessageQueueReceiverEndpoint(name = "productQueue", channelName = "${exampleservice.product-channel}")
public class ExampleConfiguration implements MessageEndpointConfigurer {

    private static final Logger LOG = getLogger(ExampleConfiguration.class);

    @Override
    public void configureMessageInterceptors(final MessageInterceptorRegistry registry) {
        registry.register(receiverChannelsWith(m -> {
            LOG.info("[receiver] Intercepted message {}", m);
            return m;
        }));
        registry.register(senderChannelsWith((m) -> {
            LOG.info("[sender] Intercepted message {}", m);
            return m;
        }));
    }

    @Bean
    public StateRepository<BananaProduct> bananaProductConcurrentStateRepository() {
        return new ConcurrentHashMapStateRepository<>();
    }

    @Bean
    public InMemoryMessageQueueSender bananaMessageSender(final InMemoryMessageQueueSenderFactory inMemoryMessageQueueSenderFactory,
                                                     final MyServiceProperties properties) {
        return inMemoryMessageQueueSenderFactory.create(properties.getBananaChannel());
    }

    @Bean
    public InMemoryMessageQueueSender productMessageSender(final InMemoryMessageQueueSenderFactory inMemoryMessageQueueSenderFactory,
                                                                  final MyServiceProperties properties) {
        return inMemoryMessageQueueSenderFactory.create(properties.getProductChannel());
    }

}
