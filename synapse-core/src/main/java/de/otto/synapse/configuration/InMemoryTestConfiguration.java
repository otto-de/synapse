package de.otto.synapse.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.InMemoryMessageSenderFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.eventsource.InMemoryMessageLogReceiverEndpointFactory;
import de.otto.synapse.messagestore.CompactingInMemoryMessageStore;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;

/**
 * Configuration used to implement tests. Use this, if you want to bypass Kinesis and other hard-to-test
 * infrastructures and replace them by in-memory implementations.
 * <p>
 *     {@code InMemoryTestConfiguration} can be activated by {@link org.springframework.boot.autoconfigure.ImportAutoConfiguration importing} it
 *     into some other {@code Configuration} class:
 * </p>
 * <pre><code>
 * &#64;Configuration
 * &#64;ImportAutoConfiguration(InMemoryTestConfiguration.class)
 * public class MyTestConfig {
 *     // ...
 * }
 * </code></pre>
 */
public class InMemoryTestConfiguration {

    @Bean
    public InMemoryChannels inMemoryChannels(final ObjectMapper objectMapper, final ApplicationEventPublisher eventPublisher) {
        return new InMemoryChannels(objectMapper, eventPublisher);
    }

    @Bean
    public MessageSenderEndpointFactory messageSenderEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                                     final InMemoryChannels inMemoryChannels, final ObjectMapper objectMapper) {
        return new InMemoryMessageSenderFactory(interceptorRegistry, inMemoryChannels, objectMapper);
    }

    @Bean
    public MessageStoreFactory<MessageStore> snapshotMessageStoreFactory() {
        return (channelName -> new CompactingInMemoryMessageStore(true));
    }

    @Bean
    public MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                                               final InMemoryChannels inMemoryChannels) {
        return new InMemoryMessageLogReceiverEndpointFactory(interceptorRegistry, inMemoryChannels);
    }

}
