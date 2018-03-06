package de.otto.synapse.testsupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.InMemoryEventSource;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;


public class InMemoryEventSourceConfiguration {

    @Bean
    public EventSourceBuilder inMemEventSourceBuilder(final ApplicationEventPublisher eventPublisher,
                                                      final ObjectMapper objectMapper) {
        return (name, streamName) -> {
            final InMemoryChannel channel = InMemoryChannels.getChannel(streamName);
            return new InMemoryEventSource(name, streamName, channel, eventPublisher, objectMapper);
        };
    }

}
