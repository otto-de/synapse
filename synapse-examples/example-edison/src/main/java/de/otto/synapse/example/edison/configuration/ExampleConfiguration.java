package de.otto.synapse.example.edison.configuration;

import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.EnableMessageQueueReceiverEndpoint;
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
                                                     final InMemoryChannels inMemoryChannels) {
        return buildMessageSender(properties.getBananaChannel(), inMemoryChannels);
    }

    @Bean
    public MessageSenderEndpoint productMessageSender(final MyServiceProperties properties,
                                                      final InMemoryChannels inMemoryChannels) {
        return buildMessageSender(properties.getProductChannel(), inMemoryChannels);
    }

    private MessageSenderEndpoint buildMessageSender(final String channelName,
                                                     final InMemoryChannels inMemoryChannels) {
        final MessageTranslator<String> translator = new JsonStringMessageTranslator();
        return new InMemoryMessageSender(interceptorRegistry, translator, inMemoryChannels.getChannel(channelName));
    }
}
