package de.otto.edison.eventsourcing.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSourceBuilder;
import de.otto.edison.eventsourcing.example.producer.configuration.MyServiceProperties;
import de.otto.edison.eventsourcing.inmemory.InMemoryChannel;
import de.otto.edison.eventsourcing.inmemory.InMemoryChannels;
import de.otto.edison.eventsourcing.inmemory.InMemoryEventSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class TestEventConsumerConfiguration {

    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final MyServiceProperties properties,
                                                        final ApplicationEventPublisher eventPublisher,
                                                        final ObjectMapper objectMapper) {
        final InMemoryChannel productStream = InMemoryChannels.getChannel(properties.getProductStreamName());
        return (name, streamName) -> new InMemoryEventSource(name, streamName, productStream, eventPublisher, objectMapper);
    }

}
