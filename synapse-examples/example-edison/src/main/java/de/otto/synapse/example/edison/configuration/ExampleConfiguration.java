package de.otto.synapse.example.edison.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.messagequeue.EnableMessageQueueReceiverEndpoint;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.configuration.InMemoryMessageQueueTestConfiguration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.InMemoryMessageSender;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.example.edison.state.BananaProduct;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@EnableConfigurationProperties({MyServiceProperties.class})
@ImportAutoConfiguration({InMemoryMessageLogTestConfiguration.class,InMemoryMessageQueueTestConfiguration.class})
@EnableEventSource(name = "bananaSource",  channelName = "${exampleservice.banana-channel}")
@EnableEventSource(name = "productSource", channelName = "${exampleservice.product-channel}")
@EnableMessageQueueReceiverEndpoint(name = "bananaQueue", channelName = "banana-queue")
public class ExampleConfiguration {

    @Autowired
    private MessageInterceptorRegistry interceptorRegistry;

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
        return new InMemoryMessageSender(interceptorRegistry, translator, inMemoryChannels.getChannel(channelName));
    }
}
