package de.otto.synapse.example.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.InMemoryEventSource;
import de.otto.synapse.example.consumer.configuration.MyServiceProperties;
import de.otto.synapse.sender.InMemoryMessageSender;
import de.otto.synapse.sender.MessageSenderFactory;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static de.otto.synapse.channel.InMemoryChannels.getChannel;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class TestEventSourcingConfiguration {

    @Bean
    public MessageSenderFactory eventSenderFactory(final ObjectMapper objectMapper,
                                                   final MyServiceProperties myServiceProperties) {
        final MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator(objectMapper);
        return streamName -> {
            if (streamName.equals(myServiceProperties.getBananaChannel())) {
                return new InMemoryMessageSender(messageTranslator, getChannel(myServiceProperties.getBananaChannel()));
            } else if (streamName.equals(myServiceProperties.getProductChannel())) {
                return new InMemoryMessageSender(messageTranslator, getChannel(myServiceProperties.getProductChannel()));
            } else {
                throw new IllegalArgumentException("no stream for name " + streamName + " available.");
            }
        };
    }


    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final MyServiceProperties myServiceProperties,
                                                        final ApplicationEventPublisher eventPublisher,
                                                        final ObjectMapper objectMapper) {
        return (name, streamName) -> {
            if (streamName.equals(myServiceProperties.getBananaChannel())) {
                return new InMemoryEventSource(name, streamName, getChannel(myServiceProperties.getBananaChannel()), eventPublisher, objectMapper);
            } else if (streamName.equals(myServiceProperties.getProductChannel())) {
                return new InMemoryEventSource(name, streamName, getChannel(myServiceProperties.getProductChannel()), eventPublisher, objectMapper);
            } else {
                throw new IllegalArgumentException("no stream for name " + streamName + " available.");
            }
        };
    }

}
