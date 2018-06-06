package de.otto.synapse.example.edison.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.configuration.InMemoryTestConfiguration;
import de.otto.synapse.endpoint.sender.InMemoryMessageSender;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.example.edison.state.BananaProduct;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@EnableConfigurationProperties({MyServiceProperties.class})
@ImportAutoConfiguration(InMemoryTestConfiguration.class)
@EnableEventSource(name = "bananaSource",  channelName = "${exampleservice.banana-channel}")
@EnableEventSource(name = "productSource", channelName = "${exampleservice.product-channel}")
public class ExampleConfiguration {

    @Bean
    public StateRepository<BananaProduct> bananaProductConcurrentStateRepository() {
        return new ConcurrentHashMapStateRepository<>();
    }

    @Bean
    public MessageSenderEndpoint bananaMessageSender(final MyServiceProperties properties,
                                                     final ObjectMapper objectMapper,
                                                     final InMemoryChannels inMemoryChannels) {
        return buildMessageSender(properties.getBananaChannel(), objectMapper, inMemoryChannels);
    }

    @Bean
    public MessageSenderEndpoint productMessageSender(final MyServiceProperties properties,
                                                      final ObjectMapper objectMapper,
                                                      final InMemoryChannels inMemoryChannels) {
        return buildMessageSender(properties.getProductChannel(), objectMapper, inMemoryChannels);
    }

    private MessageSenderEndpoint buildMessageSender(final String channelName,
                                                     final ObjectMapper objectMapper,
                                                     final InMemoryChannels inMemoryChannels) {
        final MessageTranslator<String> translator = new JsonStringMessageTranslator(objectMapper);
        return new InMemoryMessageSender(translator, inMemoryChannels.getChannel(channelName));
    }
}
