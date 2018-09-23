package de.otto.synapse.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannels;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;

public class InMemoryChannelTestConfiguration {

    @Bean
    public InMemoryChannels inMemoryChannels(final ObjectMapper objectMapper,
                                             final ApplicationEventPublisher eventPublisher) {
        return new InMemoryChannels(objectMapper, eventPublisher);
    }

}
