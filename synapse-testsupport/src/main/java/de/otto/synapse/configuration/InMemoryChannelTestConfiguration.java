package de.otto.synapse.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannels;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;

import static org.slf4j.LoggerFactory.getLogger;

public class InMemoryChannelTestConfiguration {

    // TODO: in eine testsupport lib verschieben

    @Bean
    public InMemoryChannels inMemoryChannels(final ObjectMapper objectMapper, final ApplicationEventPublisher eventPublisher) {
        return new InMemoryChannels(objectMapper, eventPublisher);
    }

}
