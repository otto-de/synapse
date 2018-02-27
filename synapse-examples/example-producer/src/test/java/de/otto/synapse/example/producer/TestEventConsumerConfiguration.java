package de.otto.synapse.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.InMemoryEventSource;
import de.otto.synapse.example.producer.configuration.MyServiceProperties;
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
