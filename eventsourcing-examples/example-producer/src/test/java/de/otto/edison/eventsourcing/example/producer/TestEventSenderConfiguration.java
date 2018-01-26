package de.otto.edison.eventsourcing.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSender;
import de.otto.edison.eventsourcing.EventSenderFactory;
import de.otto.edison.eventsourcing.example.producer.configuration.MyServiceProperties;
import de.otto.edison.eventsourcing.inmemory.InMemoryEventSender;
import de.otto.edison.eventsourcing.inmemory.InMemoryStream;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSenderFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
@Profile("test")
public class TestEventSenderConfiguration {

    @Bean
    public InMemoryStream productStream() {
        return new InMemoryStream();
    }

    @Bean
    public EventSenderFactory eventSenderFactory(final ObjectMapper objectMapper, final InMemoryStream productStream) {
        return streamName -> new InMemoryEventSender(streamName, objectMapper, productStream);
    }

}
