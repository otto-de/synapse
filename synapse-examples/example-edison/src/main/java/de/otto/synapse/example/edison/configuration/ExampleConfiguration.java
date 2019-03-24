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
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.translator.MessageTranslator;
import de.otto.synapse.translator.TextMessageTranslator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@EnableConfigurationProperties({MyServiceProperties.class})
@Import({InMemoryMessageLogTestConfiguration.class,InMemoryMessageQueueTestConfiguration.class})
@EnableEventSource(name = "bananaSource",  channelName = "${exampleservice.banana-channel}")
@EnableEventSource(name = "productSource", channelName = "${exampleservice.product-channel}")
@EnableMessageQueueReceiverEndpoint(name = "bananaQueue", channelName = "banana-queue")
public class ExampleConfiguration {

    @Autowired
    private MessageInterceptorRegistry interceptorRegistry;

    @Bean
    public StateRepository<BananaProduct> bananaProductStateRepository() {
        return new ConcurrentHashMapStateRepository<>("bananaProducts");
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
        final MessageTranslator<TextMessage> translator = new TextMessageTranslator();
        return new InMemoryMessageSender(interceptorRegistry, translator, inMemoryChannels.getChannel(channelName));
    }
}
