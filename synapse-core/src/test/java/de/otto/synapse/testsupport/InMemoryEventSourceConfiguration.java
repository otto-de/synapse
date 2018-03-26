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
    public InMemoryChannels inMemoryChannels(final ObjectMapper objectMapper) {
        return new InMemoryChannels(objectMapper);
    }

    @Bean
    public EventSourceBuilder inMemEventSourceBuilder(final ApplicationEventPublisher eventPublisher,
                                                      final InMemoryChannels inMemoryChannels) {
        return (name, channelName) -> {
            final InMemoryChannel channel = inMemoryChannels.getChannel(channelName);
            return new InMemoryEventSource(name, channel, eventPublisher);
        };
    }

}
