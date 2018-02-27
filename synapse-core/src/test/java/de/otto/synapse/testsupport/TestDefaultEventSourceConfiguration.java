package de.otto.synapse.testsupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.InMemoryEventSource;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;


public class TestDefaultEventSourceConfiguration {

    @Bean
    public InMemoryChannel inMemoryStream() {
        return new InMemoryChannel();
    }

    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final ApplicationEventPublisher eventPublisher,
                                                        final ObjectMapper objectMapper) {
        return new TestDefaultEventSourceBuilder(inMemoryStream(), eventPublisher, objectMapper);
    }

    public static class TestEventSource extends InMemoryEventSource {

        public TestEventSource(String name,
                               String streamName,
                               InMemoryChannel inMemoryChannel,
                               ApplicationEventPublisher eventPublisher,
                               ObjectMapper objectMapper) {
            super(name, streamName, inMemoryChannel, eventPublisher, objectMapper);
        }
    }

    public static class TestDefaultEventSourceBuilder implements EventSourceBuilder {

        private final InMemoryChannel inMemoryChannel;
        private final ApplicationEventPublisher eventPublisher;
        private final ObjectMapper objectMapper;

        public TestDefaultEventSourceBuilder(final InMemoryChannel inMemoryChannel,
                                             final ApplicationEventPublisher eventPublisher,
                                             final ObjectMapper objectMapper) {
            this.inMemoryChannel = inMemoryChannel;

            this.eventPublisher = eventPublisher;
            this.objectMapper = objectMapper;
        }

        @Override
        public EventSource buildEventSource(String name, String streamName) {
            return new TestEventSource(name, streamName, inMemoryChannel, eventPublisher, objectMapper);
        }
    }

}
