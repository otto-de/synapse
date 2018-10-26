package de.otto.synapse.example.consumer.configuration;

import de.otto.synapse.annotation.EnableMessageQueueReceiverEndpoint;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.configuration.InMemoryMessageQueueTestConfiguration;
import de.otto.synapse.configuration.MessageEndpointConfigurer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.example.consumer.state.BananaProduct;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.receiverChannelsWith;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.senderChannelsWith;
import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@ImportAutoConfiguration(InMemoryMessageQueueTestConfiguration.class)
@EnableConfigurationProperties({MyServiceProperties.class})
@EnableMessageQueueReceiverEndpoint(name = "bananaQueue",  channelName = "${exampleservice.banana-channel}")
@EnableMessageQueueReceiverEndpoint(name = "productQueue", channelName = "${exampleservice.product-channel}")
@EnableMessageSenderEndpoint(name = "productMessageSender", channelName = "${exampleservice.product-channel}")
@EnableMessageSenderEndpoint(name = "bananaMessageSender", channelName = "${exampleservice.banana-channel}")
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

}
