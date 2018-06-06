package de.otto.synapse.edison.trace;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.messagestore.InMemoryRingBufferMessageStore;
import de.otto.synapse.messagestore.MessageStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableBiMap.builder;
import static java.util.Collections.emptyList;

@Configuration
@ConditionalOnProperty(
        prefix = "synapse.edison.trace",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class MessageTraceAutoConfiguration {

    @Autowired
    private MessageInterceptorRegistry interceptorRegistry;
    @Value("${synapse.edison.trace.capacity:100}")
    private int capacity = 100;

    @Bean
    @ConditionalOnMissingBean
    public MessageTraces traceMessageStore(final Optional<List<MessageLogReceiverEndpoint>> messageLogReceiverEndpoints,
                                           final Optional<List<MessageSenderEndpoint>> senderEndpoints) {
        return new MessageTraces(
                receiverStoresFor(messageLogReceiverEndpoints.orElse(emptyList())),
                senderStoresFor(senderEndpoints.orElse(emptyList()))
        );
    }

    private ImmutableMap<String, MessageStore> receiverStoresFor(final List<MessageLogReceiverEndpoint> messageLogReceiverEndpoints) {
        final ImmutableMap.Builder<String,MessageStore> receiverStores = builder();
        messageLogReceiverEndpoints
                .forEach(messageLogReceiverEndpoint -> {
                    final InMemoryRingBufferMessageStore messageStore = new InMemoryRingBufferMessageStore(capacity);
                    messageLogReceiverEndpoint.getInterceptorChain().register(
                            message -> {
                                messageStore.add(message);
                                return message;
                            }
                    );
                    receiverStores.put(messageLogReceiverEndpoint.getChannelName(), messageStore);
                });
        return receiverStores.build();
    }

    private ImmutableMap<String, MessageStore> senderStoresFor(final List<MessageSenderEndpoint> senderEndpoints) {
        final ImmutableMap.Builder<String,MessageStore> senderStores = builder();
        senderEndpoints
                .forEach(senderEndpoint -> {
                    final InMemoryRingBufferMessageStore messageStore = new InMemoryRingBufferMessageStore(capacity);
                    senderEndpoint.getInterceptorChain().register(message -> {
                        messageStore.add(message);
                        return message;
                    });
                    senderStores.put(senderEndpoint.getChannelName(), messageStore);
                });
        return senderStores.build();
    }
}
