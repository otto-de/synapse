package de.otto.edison.eventsourcing.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.MessageSenderFactory;
import de.otto.edison.eventsourcing.example.producer.configuration.MyServiceProperties;
import de.otto.edison.eventsourcing.inmemory.InMemoryMessageSender;
import de.otto.edison.eventsourcing.inmemory.InMemoryStream;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class TestEventSenderConfiguration {

    @Bean
    public InMemoryStream productStream() {
        return new InMemoryStream();
    }

    @Bean
    public MessageSenderFactory eventSenderFactory(final ObjectMapper objectMapper, final InMemoryStream productStream) {
        return streamName -> new InMemoryMessageSender(objectMapper, productStream);
    }

}
