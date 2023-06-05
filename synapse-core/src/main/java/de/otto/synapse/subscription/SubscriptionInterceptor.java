package de.otto.synapse.subscription;

import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.TextMessage;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

public class SubscriptionInterceptor implements MessageInterceptor {
    private final Subscription subscription;
    private final MessageSenderEndpoint targetSenderEndpoint;

    public SubscriptionInterceptor(final Subscription subscription,
                                   final MessageSenderEndpoint targetSenderEndpoint) {
        this.subscription = subscription;
        this.targetSenderEndpoint = targetSenderEndpoint;
    }

    Subscription getSubscription() {
        return subscription;
    }

    @Nullable
    @Override
    public TextMessage intercept(@Nonnull TextMessage message) {
        if (subscription.getSubscribedEntities().contains(message.getKey().partitionKey())) {
            targetSenderEndpoint.send(message);
        }
        return message;
    }
}
