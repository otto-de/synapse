package de.otto.edison.eventsourcing.example.consumer.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSourceBuilder;
import de.otto.edison.eventsourcing.MessageSender;
import de.otto.edison.eventsourcing.annotation.EnableEventSource;
import de.otto.edison.eventsourcing.example.consumer.state.BananaProduct;
import de.otto.edison.eventsourcing.inmemory.InMemoryEventSource;
import de.otto.edison.eventsourcing.message.Message;
import de.otto.edison.eventsourcing.state.ConcurrentHashMapStateRepository;
import de.otto.edison.eventsourcing.state.StateRepository;
import de.otto.edison.eventsourcing.translator.JsonStringMessageTranslator;
import de.otto.edison.eventsourcing.translator.MessageTranslator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static de.otto.edison.eventsourcing.inmemory.InMemoryChannels.getChannel;

@Configuration
@EnableConfigurationProperties({MyServiceProperties.class})
@EnableEventSource(name = "bananaSource",  streamName = "${exampleservice.banana-channel}")
@EnableEventSource(name = "productSource", streamName = "${exampleservice.product-channel}")
public class ExampleConfiguration {

    @Bean
    public StateRepository<BananaProduct> bananaProductStateRepository() {
        return new ConcurrentHashMapStateRepository<>();
    }

    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final ApplicationEventPublisher eventPublisher,
                                                        final ObjectMapper objectMapper) {
        return (name, streamName) -> new InMemoryEventSource(name, streamName, getChannel(streamName), eventPublisher, objectMapper);
    }

    @Bean
    public MessageSender bananaMessageSender(final MyServiceProperties properties,
                                             final ObjectMapper objectMapper) {
        return buildMessageSender(properties.getBananaChannel(), objectMapper);
    }

    @Bean
    public MessageSender productMessageSender(final MyServiceProperties properties,
                                              final ObjectMapper objectMapper) {
        return buildMessageSender(properties.getProductChannel(), objectMapper);
    }

    private MessageSender buildMessageSender(final String channelName,
                                             final ObjectMapper objectMapper) {
        MessageTranslator<String> translator = new JsonStringMessageTranslator(objectMapper);
        return new MessageSender() {
            @Override
            public <T> void send(Message<T> message) {
                getChannel(channelName).send(translator.translate(message));
            }
        };
    }

}
