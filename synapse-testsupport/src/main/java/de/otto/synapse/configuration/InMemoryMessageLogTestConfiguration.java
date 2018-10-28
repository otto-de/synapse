package de.otto.synapse.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.InMemoryMessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.InMemoryMessageSenderFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.messagestore.CompactingInMemoryMessageStore;
import de.otto.synapse.messagestore.DelegatingSnapshotMessageStore;
import de.otto.synapse.messagestore.MessageStoreFactory;
import de.otto.synapse.messagestore.SnapshotMessageStore;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Configuration used to implement tests. Use this, if you want to bypass Kinesis and other hard-to-test
 * infrastructures and replace them by in-memory implementations.
 * <p>
 * {@code InMemoryTestConfiguration} can be activated by {@link org.springframework.boot.autoconfigure.ImportAutoConfiguration importing} it
 * into some other {@code Configuration} class:
 * </p>
 * <pre><code>
 * &#64;Configuration
 * &#64;ImportAutoConfiguration(InMemoryMessageLogTestConfiguration.class)
 * public class MyTestConfig {
 *     // ...
 * }
 * </code></pre>
 */
@ImportAutoConfiguration(InMemoryChannelTestConfiguration.class)
public class InMemoryMessageLogTestConfiguration {

    private static final Logger LOG = getLogger(InMemoryMessageLogTestConfiguration.class);

    @Bean
    public MessageSenderEndpointFactory messageLogSenderEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                                            final InMemoryChannels inMemoryChannels,
                                                                            final ObjectMapper objectMapper) {
        LOG.warn("Creating InMemoryMessageSenderEndpointFactory. This should only be used in tests");
        return new InMemoryMessageSenderFactory(interceptorRegistry, inMemoryChannels, objectMapper);
    }

    @Bean
    public MessageStoreFactory<SnapshotMessageStore> snapshotMessageStoreFactory() {
        return (channelName -> new DelegatingSnapshotMessageStore(
                new CompactingInMemoryMessageStore(true))
        );
    }

    @Bean
    public MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory(final InMemoryChannels inMemoryChannels) {
        LOG.warn("Creating InMemoryMessageLogReceiverEndpointFactory. This should only be used in tests");
        return new InMemoryMessageLogReceiverEndpointFactory(inMemoryChannels);
    }

}
