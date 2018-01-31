package de.otto.edison.eventsourcing.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSourceBuilder;
import de.otto.edison.eventsourcing.inmemory.InMemoryEventSource;
import de.otto.edison.eventsourcing.inmemory.InMemoryStream;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TestEventConsumerConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "defaultEventSourceBuilder")
    public EventSourceBuilder defaultEventSourceBuilder(final InMemoryStream productStream,
                                                        final ObjectMapper objectMapper) {
        return (name, streamName) -> new InMemoryEventSource(name, productStream, objectMapper);
    }

}
