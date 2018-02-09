package de.otto.edison.eventsourcing.testsupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSourceBuilder;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.inmemory.InMemoryEventSource;
import de.otto.edison.eventsourcing.inmemory.InMemoryStream;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;


public class TestDefaultEventSourceConfiguration {

    @Bean
    public InMemoryStream inMemoryStream() {
        return new InMemoryStream();
    }

    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final ApplicationEventPublisher eventPublisher,
                                                        final ObjectMapper objectMapper) {
        return new TestDefaultEventSourceBuilder(inMemoryStream(), eventPublisher, objectMapper);
    }

    public static class TestEventSource extends InMemoryEventSource {

        public TestEventSource(String name,
                               String streamName,
                               InMemoryStream inMemoryStream,
                               ApplicationEventPublisher eventPublisher,
                               ObjectMapper objectMapper) {
            super(name, streamName, inMemoryStream, eventPublisher, objectMapper);
        }
    }

    public static class TestDefaultEventSourceBuilder implements EventSourceBuilder {

        private final InMemoryStream inMemoryStream;
        private final ApplicationEventPublisher eventPublisher;
        private final ObjectMapper objectMapper;

        public TestDefaultEventSourceBuilder(final InMemoryStream inMemoryStream,
                                             final ApplicationEventPublisher eventPublisher,
                                             final ObjectMapper objectMapper) {
            this.inMemoryStream = inMemoryStream;

            this.eventPublisher = eventPublisher;
            this.objectMapper = objectMapper;
        }

        @Override
        public EventSource buildEventSource(String name, String streamName) {
            return new TestEventSource(name, streamName, inMemoryStream, eventPublisher, objectMapper);
        }
    }

}
