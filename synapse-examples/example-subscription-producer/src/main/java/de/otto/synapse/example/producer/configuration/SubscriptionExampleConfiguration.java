package de.otto.synapse.example.producer.configuration;

import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.channel.selector.Kafka;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.configuration.MessageEndpointConfigurer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.example.producer.Product;
import de.otto.synapse.state.ConcurrentMapStateRepository;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.subscription.SnapshotProvider;
import de.otto.synapse.subscription.StateRepositorySnapshotProvider;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.scheduling.annotation.EnableScheduling;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.receiverChannelsWith;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.senderChannelsWith;
import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@Import(InMemoryMessageLogTestConfiguration.class)
// the message sender used to send product updates to some random channel. Kafka is used as a transport channel.
@EnableMessageSenderEndpoint(
        name = "messageSender",
        channelName = "${exampleservice.product-channel-name}",
        selector = Kafka.class)
// the event source used to receive subscription messages. In this case, Kafka is used as source of events.
@EnableEventSource(
        name = "subscriptions",
        channelName = "${exampleservice.subscription-channel-name}",
        selector = Kafka.class)
@EnableScheduling
public class SubscriptionExampleConfiguration implements MessageEndpointConfigurer {

    private static final Logger LOG = getLogger(SubscriptionExampleConfiguration.class);
    @Value("${exampleservice.product-channel-name}")
    private String productChannelName;

    /**
     * Configures some interceptors just to log the messages.
     *
     * @param registry MessageInterceptorRegistry used to register the interceptors.
     */
    @Override
    public void configureMessageInterceptors(final MessageInterceptorRegistry registry) {
        registry.register(senderChannelsWith((message) -> {
            LOG.info("Sending message {}", message);
            return message;
        }, Ordered.HIGHEST_PRECEDENCE));
        registry.register(receiverChannelsWith((message) -> {
            LOG.info("Received message {}", message);
            return message;
        }, Ordered.LOWEST_PRECEDENCE));
    }

    /**
     * A StateRepository used to store products.
     * @return StateRepository
     */
    @Bean
    public StateRepository<Product> productStateRepository() {
        return new ConcurrentMapStateRepository<>(productChannelName);
    }

    /**
     * A SnapshotProvider that is generating the snapshot messages from the current state
     * of the entities stored in the StateRepository.
     * The messages will contain the entity as the message payload. Instead of this,
     * the StateRepositorySnapshotProvider could be configured with an {@link de.otto.synapse.subscription.EntityToMessageListTransformer},
     * used to generate one or more messages for a single entity.
     * @param productStateRepository
     * @return
     */
    @Bean
    public SnapshotProvider productSnapshotProvider(final StateRepository<Product> productStateRepository) {
        return new StateRepositorySnapshotProvider<>(productChannelName, productStateRepository);
    }

}
