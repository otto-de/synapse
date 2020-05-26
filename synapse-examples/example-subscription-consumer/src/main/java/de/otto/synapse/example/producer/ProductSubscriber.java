package de.otto.synapse.example.producer;

import com.google.common.collect.ImmutableSet;
import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.subscription.events.SubscriptionCreated;
import de.otto.synapse.subscription.events.SubscriptionUpdated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static de.otto.synapse.message.Message.message;

/**
 * A component that is subscribing to products 2, 4, 6, 8 and 10. All other products will not be consumed
 * by this service.
 */
@Component
public class ProductSubscriber {

    private final static Logger LOG = LoggerFactory.getLogger(ProductSubscriber.class);

    private final StateRepository<Product> productRepository;

    @Autowired
    public ProductSubscriber(final StateRepository<Product> productRepository,
                             final MessageSenderEndpoint subscriptionSender,
                             final @Value("${exampleservice.product-channel-name}") String productsChannel,
                             final @Value("${exampleservice.subscribed-products-channel-name}") String subscribedProductsChannel) {
        this.productRepository = productRepository;

        final String subscriptionId = "subscription-1";
        // First create a subscription for the products channel:
        subscriptionSender.send(message(subscriptionId, new SubscriptionCreated(
                subscriptionId,
                productsChannel,
                subscribedProductsChannel))).join();
        // Now we send an update, telling the producer about the products we are interested in.
        // This can later be changed by sending more update events.
        subscriptionSender.send(message(subscriptionId, new SubscriptionUpdated(
                subscriptionId,
                // newly subscribed products:
                ImmutableSet.of("2", "4", "6", "8", "10"),
                // this could be used to unsubscribe other products:
                ImmutableSet.of()))).join();
    }

    /** The consumer that will get the updates for the subscribed products */
    @EventSourceConsumer(eventSource = "productSource", payloadType = Product.class)
    public void consumeProducts(final Message<Product> message) {
        productRepository.put(message.getKey().partitionKey(), message.getPayload());
    }

}
