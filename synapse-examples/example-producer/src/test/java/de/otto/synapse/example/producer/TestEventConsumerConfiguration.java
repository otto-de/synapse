package de.otto.synapse.example.producer;

import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.InMemoryEventSourceBuilder;
import de.otto.synapse.example.producer.configuration.MyServiceProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class TestEventConsumerConfiguration {

    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final MessageInterceptorRegistry interceptorRegistry,
                                                        final InMemoryChannels inMemoryChannels) {
        return new InMemoryEventSourceBuilder(interceptorRegistry, inMemoryChannels);
    }

}
