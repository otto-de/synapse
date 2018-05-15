package de.otto.synapse.testsupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.InMemoryEventSourceBuilder;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;


public class InMemoryEventSourceConfiguration {
    @Bean
    public InMemoryChannels inMemoryChannels(final ObjectMapper objectMapper) {
        return new InMemoryChannels(objectMapper);
    }

    @Bean
    public EventSourceBuilder inMemEventSourceBuilder(final MessageInterceptorRegistry interceptorRegistry,
                                                        final ApplicationEventPublisher eventPublisher,
                                                        final InMemoryChannels inMemoryChannels) {
        return new InMemoryEventSourceBuilder(interceptorRegistry, inMemoryChannels, eventPublisher);
    }


}
