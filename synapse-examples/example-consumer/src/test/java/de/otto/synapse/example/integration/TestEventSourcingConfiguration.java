package de.otto.synapse.example.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.InMemoryMessageSender;
import de.otto.synapse.endpoint.sender.MessageSenderFactory;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.InMemoryEventSourceBuilder;
import de.otto.synapse.example.consumer.configuration.MyServiceProperties;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class TestEventSourcingConfiguration {

    @Bean
    public MessageSenderFactory eventSenderFactory(final ObjectMapper objectMapper,
                                                   final MyServiceProperties myServiceProperties,
                                                   final InMemoryChannels inMemoryChannels) {
        final MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator(objectMapper);
        return channelName -> {
            if (channelName.equals(myServiceProperties.getBananaChannel())) {
                return new InMemoryMessageSender(messageTranslator, inMemoryChannels.getChannel(myServiceProperties.getBananaChannel()));
            } else if (channelName.equals(myServiceProperties.getProductChannel())) {
                return new InMemoryMessageSender(messageTranslator, inMemoryChannels.getChannel(myServiceProperties.getProductChannel()));
            } else {
                throw new IllegalArgumentException("no channel for name " + channelName + " available.");
            }
        };
    }

    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final MessageInterceptorRegistry interceptorRegistry,
                                                        final ApplicationEventPublisher eventPublisher,
                                                        final InMemoryChannels inMemoryChannels) {
        return new InMemoryEventSourceBuilder(interceptorRegistry, inMemoryChannels, eventPublisher);
    }


}
