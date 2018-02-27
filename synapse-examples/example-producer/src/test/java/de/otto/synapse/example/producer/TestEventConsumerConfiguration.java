package de.otto.synapse.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.EventSourceBuilder;
import de.otto.synapse.example.producer.configuration.MyServiceProperties;
import de.otto.synapse.inmemory.InMemoryChannel;
import de.otto.synapse.inmemory.InMemoryChannels;
import de.otto.synapse.inmemory.InMemoryEventSource;
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
