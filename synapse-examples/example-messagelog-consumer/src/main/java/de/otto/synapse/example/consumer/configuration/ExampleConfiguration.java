package de.otto.synapse.example.consumer.configuration;

import de.otto.synapse.annotation.EnableMessageLogReceiverEndpoint;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.configuration.MessageEndpointConfigurer;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.example.consumer.state.BananaProduct;
import de.otto.synapse.state.ChronicleMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.slf4j.LoggerFactory.getLogger;


@Configuration
@Import(InMemoryMessageLogTestConfiguration.class)
@EnableConfigurationProperties({MyServiceProperties.class})
@EnableMessageLogReceiverEndpoint(name = "bananaSource", channelName = "${exampleservice.banana-channel}")
@EnableMessageLogReceiverEndpoint(name = "productSource", channelName = "${exampleservice.product-channel}")
public class ExampleConfiguration implements MessageEndpointConfigurer {

    private static final Logger LOG = getLogger(ExampleConfiguration.class);

    @Bean
    public StateRepository<BananaProduct> bananaProductStateRepository() {
        return ChronicleMapStateRepository.builder(BananaProduct.class).withName("bananaProducts").build();
    }

    @Bean
    public MessageSenderEndpoint bananaMessageSender(final MessageSenderEndpointFactory messageLogSenderEndpointFactory,
                                                     final MyServiceProperties properties) {
        return messageLogSenderEndpointFactory.create(properties.getBananaChannel());
    }

    @Bean
    public MessageSenderEndpoint productMessageSender(final MessageSenderEndpointFactory messageLogSenderEndpointFactory,
                                                      final MyServiceProperties properties) {
        return messageLogSenderEndpointFactory.create(properties.getProductChannel());
    }

}
