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
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.receiverChannelsWith;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.senderChannelsWith;
import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@Import(InMemoryMessageLogTestConfiguration.class)
// the sender used to send subscription messages to the example-subscription-producer
@EnableMessageSenderEndpoint(
        name = "subscriptionSender",
        channelName = "${exampleservice.subscription-channel-name}",
        selector = Kafka.class)
// the event source used to receive updates for subscribed product entities
@EnableEventSource(
        name = "productSource",
        channelName = "${exampleservice.subscribed-products-channel-name}",
        selector = Kafka.class)
@EnableScheduling
public class SubscriptionExampleConfiguration implements MessageEndpointConfigurer {

    private static final Logger LOG = getLogger(SubscriptionExampleConfiguration.class);

    /** logs the messages sent to and received from the producer */
    @Override
    public void configureMessageInterceptors(final MessageInterceptorRegistry registry) {
        registry.register(senderChannelsWith((message) -> {
            LOG.info("Sending message {}", message);
            return message;
        }));
        registry.register(receiverChannelsWith((message) -> {
            LOG.info("Received message {}", message);
            return message;
        }));
    }

    /**
     * Some state repository used to store the products.
     *
     * @return
     */
    @Bean
    public StateRepository<Product> productStateRepository() {
        return new ConcurrentMapStateRepository<>("products");
    }

}
