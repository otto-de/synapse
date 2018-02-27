package de.otto.edison.eventsourcing.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.MessageSenderFactory;
import de.otto.edison.eventsourcing.example.producer.configuration.MyServiceProperties;
import de.otto.edison.eventsourcing.inmemory.InMemoryChannel;
import de.otto.edison.eventsourcing.inmemory.InMemoryChannels;
import de.otto.edison.eventsourcing.inmemory.InMemoryMessageSender;
import de.otto.edison.eventsourcing.translator.JsonStringMessageTranslator;
import de.otto.edison.eventsourcing.translator.MessageTranslator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class TestMessageSenderConfiguration {

    @Bean
    public MessageSenderFactory messageSenderFactory(final ObjectMapper objectMapper,
                                                     final MyServiceProperties properties) {
        final InMemoryChannel productStream = InMemoryChannels.getChannel(properties.getProductStreamName());
        final MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator(objectMapper);
        return streamName -> new InMemoryMessageSender(messageTranslator, productStream);
    }

}
