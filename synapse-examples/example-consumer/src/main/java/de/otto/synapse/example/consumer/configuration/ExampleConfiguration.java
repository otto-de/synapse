package de.otto.synapse.example.consumer.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.sender.InMemoryMessageSender;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.InMemoryEventSource;
import de.otto.synapse.example.consumer.state.BananaProduct;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@EnableConfigurationProperties({MyServiceProperties.class})
@EnableEventSource(name = "bananaSource",  channelName = "${exampleservice.banana-channel}")
@EnableEventSource(name = "productSource", channelName = "${exampleservice.product-channel}")
public class ExampleConfiguration {

    @Bean
    public InMemoryChannels inMemoryChannels(final ObjectMapper objectMapper) {
        return new InMemoryChannels(objectMapper);
    }

    @Bean
    public StateRepository<BananaProduct> bananaProductConcurrentStateRepository() {
        return new ConcurrentHashMapStateRepository<>();
    }

    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final ApplicationEventPublisher eventPublisher,
                                                        final InMemoryChannels inMemoryChannels) {
        return (name, channelName) -> {
            return new InMemoryEventSource(name, inMemoryChannels.getChannel(channelName), eventPublisher);
        };
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
