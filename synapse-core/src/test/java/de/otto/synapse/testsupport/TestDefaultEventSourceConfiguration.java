package de.otto.synapse.testsupport;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.InMemoryEventSource;
import org.springframework.context.annotation.Bean;


public class TestDefaultEventSourceConfiguration {

    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final InMemoryChannels inMemoryChannels) {
        return (name, channelName) -> {
            final InMemoryChannel channel = inMemoryChannels.getChannel(channelName);
            return new TestEventSource(name, channel);

        };
    }

    public static class TestEventSource extends InMemoryEventSource {

        public TestEventSource(String name,
                               InMemoryChannel inMemoryChannel) {
            super(name, inMemoryChannel);
        }
    }

}
