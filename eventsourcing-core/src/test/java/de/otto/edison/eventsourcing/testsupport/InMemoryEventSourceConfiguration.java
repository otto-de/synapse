package de.otto.edison.eventsourcing.testsupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSourceBuilder;
import de.otto.edison.eventsourcing.inmemory.InMemoryEventSource;
import de.otto.edison.eventsourcing.inmemory.InMemoryStream;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;


public class InMemoryEventSourceConfiguration {

    @Bean
    public InMemoryStream inMemoryStream() {
        return new InMemoryStream();
    }

    @Bean
    public EventSourceBuilder inMemEventSourceBuilder(final ApplicationEventPublisher eventPublisher,
                                                      final ObjectMapper objectMapper) {
        return (name, streamName) -> new InMemoryEventSource(name, inMemoryStream(), eventPublisher, objectMapper);
    }

}
