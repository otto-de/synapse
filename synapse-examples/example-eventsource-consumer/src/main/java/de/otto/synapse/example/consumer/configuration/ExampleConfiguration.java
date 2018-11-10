package de.otto.synapse.example.consumer.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.configuration.MessageEndpointConfigurer;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.example.consumer.state.BananaProduct;
import de.otto.synapse.state.ChronicleMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.slf4j.LoggerFactory.getLogger;


@Configuration
@ImportAutoConfiguration(InMemoryMessageLogTestConfiguration.class)
@EnableConfigurationProperties({MyServiceProperties.class})
@EnableEventSource(name = "bananaSource", channelName = "${exampleservice.banana-channel}")
@EnableEventSource(name = "productSource", channelName = "${exampleservice.product-channel}")
public class ExampleConfiguration implements MessageEndpointConfigurer {

    private static final Logger LOG = getLogger(ExampleConfiguration.class);

    @Bean
    public StateRepository<BananaProduct> bananaProductConcurrentStateRepository(final ObjectMapper objectMapper) {
        return ChronicleMapStateRepository.builder(BananaProduct.class).withObjectMapper(objectMapper).build();
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
