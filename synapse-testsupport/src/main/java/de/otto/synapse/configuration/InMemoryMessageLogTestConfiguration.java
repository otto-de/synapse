package de.otto.synapse.configuration;

import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.InMemoryMessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.InMemoryMessageSenderFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.messagestore.DelegatingSnapshotMessageStore;
import de.otto.synapse.messagestore.MessageStoreFactory;
import de.otto.synapse.messagestore.OnHeapCompactingMessageStore;
import de.otto.synapse.messagestore.SnapshotMessageStore;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Configuration used to implement tests. Use this, if you want to bypass Kinesis and other hard-to-test
 * infrastructures and replace them by in-memory implementations.
 * <p>
 * {@code InMemoryTestConfiguration} can be activated by {@link Import importing} it
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
@Import(InMemoryChannelTestConfiguration.class)
public class InMemoryMessageLogTestConfiguration {

    private static final Logger LOG = getLogger(InMemoryMessageLogTestConfiguration.class);

    @Bean(name = {"messageLogSenderEndpointFactory", "kinesisMessageLogSenderEndpointFactory"})
    public MessageSenderEndpointFactory messageLogSenderEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                                        final InMemoryChannels inMemoryChannels) {
        LOG.warn("Creating in-memory messageLogSenderEndpointFactory. This should only be used in tests");
        return new InMemoryMessageSenderFactory(interceptorRegistry, inMemoryChannels, MessageLog.class);
    }

    @Bean
    public MessageStoreFactory<SnapshotMessageStore> snapshotMessageStoreFactory() {
        return (channelName -> new DelegatingSnapshotMessageStore(
                new OnHeapCompactingMessageStore(true))
        );
    }

    @Bean
    public MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory(final InMemoryChannels inMemoryChannels) {
        LOG.warn("Creating InMemoryMessageLogReceiverEndpointFactory. This should only be used in tests");
        return new InMemoryMessageLogReceiverEndpointFactory(inMemoryChannels, MessageLog.class);
    }

}
