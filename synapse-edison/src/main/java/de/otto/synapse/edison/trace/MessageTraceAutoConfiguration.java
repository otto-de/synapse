package de.otto.synapse.edison.trace;

import de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.emptyList;

@Configuration
@ConditionalOnProperty(
        prefix = "synapse.edison.trace",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class MessageTraceAutoConfiguration {

    @Value("${synapse.edison.trace.capacity:500}")
    private int capacity = 500;

    @Bean
    @ConditionalOnMissingBean
    public MessageTrace messageTrace(final Optional<List<MessageReceiverEndpoint>> messageReceiverEndpoints,
                                     final Optional<List<MessageSenderEndpoint>> senderEndpoints) {

        return new MessageTrace(capacity,
                concat(
                        messageReceiverEndpoints.orElse(emptyList()),
                        senderEndpoints.orElse(emptyList())
                )
        );
    }
}
