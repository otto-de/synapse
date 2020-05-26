package de.otto.synapse.example.producer;

import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.channel.selector.Kafka;
import de.otto.synapse.message.Message;
import de.otto.synapse.subscription.SubscriptionService;
import de.otto.synapse.subscription.events.SubscriptionEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * The consumer used to process subscription events.
 * This will later be moved into synapse-core, when support for subscriptions will be supported
 * using annotations or other ways to make configuration of subscriptions way easier.
 */
@Service
public class SubscriptionConsumer {
    @Autowired
    private SubscriptionService subscriptionService;

    @EventSourceConsumer(eventSource = "subscriptions", payloadType = SubscriptionEvent.class)
    public void consumeSubscriptions(final Message<SubscriptionEvent> message) {
        final SubscriptionEvent payload = message.getPayload();
        if (payload != null) {
            switch (payload.getType()) {
                case CREATED:
                    // TODO: selector woher?? Vom Source Channel?!
                    subscriptionService.onSubscriptionCreated(payload.asSubscriptionCreated(), Kafka.class);
                    break;
                case UPDATED:
                    subscriptionService.onSubscriptionUpdated(payload.asSubscriptionUpdated());
                    break;
            }
        } else {
            subscriptionService.onSubscriptionDeleted(message.getKey().partitionKey());
        }
    }
}
