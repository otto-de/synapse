package de.otto.synapse.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;

public class InMemoryChannelTestConfiguration {

    @Bean
    public InMemoryChannels inMemoryChannels(final MessageInterceptorRegistry interceptorRegistry,
                                             final ObjectMapper objectMapper,
                                             final ApplicationEventPublisher eventPublisher) {
        return new InMemoryChannels(interceptorRegistry, objectMapper, eventPublisher);
    }

}
