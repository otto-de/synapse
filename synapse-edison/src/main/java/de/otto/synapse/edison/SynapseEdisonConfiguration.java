package de.otto.synapse.edison;

import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageReceiverEndpointInfoProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Optional;

@Configuration
public class SynapseEdisonConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public MessageReceiverEndpointInfoProvider messageReceiverEndpointInfoProvider(Optional<List<EventSource>> optionalEventSources) {
        return new MessageReceiverEndpointInfoProvider(optionalEventSources);
    }
}
