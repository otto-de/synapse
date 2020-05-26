package de.otto.synapse.subscription;

import com.google.common.collect.Maps;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.BestMatchingSelectableComparator;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.message.Message;
import de.otto.synapse.subscription.events.SubscriptionCreated;
import de.otto.synapse.subscription.events.SubscriptionUpdated;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.matchingSenderChannelsWith;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

@Service
@ConditionalOnBean({
        MessageSenderEndpoint.class,
        SnapshotProvider.class
})
public class SubscriptionService {
    private static final Logger LOG = getLogger(SubscriptionService.class);

    private final MessageInterceptorRegistry registry;
    private final List<MessageSenderEndpointFactory> senderEndpointFactories;
    private final Map<String, SnapshotProvider> snapshotProviders;
    private final ConcurrentMap<String, MessageSenderEndpoint> targetSenders = new ConcurrentHashMap<>();
    private final Subscriptions subscriptions = new Subscriptions();

    public SubscriptionService(final MessageInterceptorRegistry registry,
                               final List<MessageSenderEndpointFactory> senderEndpointFactories,
                               final List<SnapshotProvider> snapshotProviders) {
        LOG.info("Initializing SubscriptionService for " + snapshotProviders.stream().map(SnapshotProvider::channelName).collect(toList()));
        this.registry = registry;
        this.senderEndpointFactories = senderEndpointFactories;
        this.snapshotProviders = Maps.uniqueIndex(snapshotProviders, SnapshotProvider::channelName);
    }

    public void onSubscriptionCreated(final SubscriptionCreated subscriptionCreated,
                                      final Class<? extends Selector> sourceChannelSelector) {
        try {
            final Class<? extends Selector> targetChannelSelector = sourceChannelSelector;
            final SnapshotProvider snapshotProvider = snapshotProviders.get(subscriptionCreated.getSubscribedChannel());
            if (snapshotProvider == null) {
                throw new IllegalArgumentException("No SnapshopProvider configured for channel " + subscriptionCreated.getSubscribedChannel());
            }

            final Subscription subscription = new Subscription(
                    subscriptionCreated.getId(),
                    subscriptionCreated.getSubscribedChannel(),
                    subscriptionCreated.getResponseChannel());
            subscriptions.addIfMissing(subscription);

            targetSenders.computeIfAbsent(subscriptionCreated.getResponseChannel(), (channelName) -> {
                final MessageSenderEndpointFactory senderEndpointFactory = senderEndpointFactories
                        .stream()
                        .filter(candiate -> candiate.matches(targetChannelSelector))
                        .min(new BestMatchingSelectableComparator(targetChannelSelector))
                        .orElseThrow(() -> new IllegalArgumentException("Unable to subscribe to " + subscriptionCreated.getSubscribedChannel() + " because no matching sender factory was found."));
                return senderEndpointFactory.create(subscriptionCreated.getResponseChannel());
            });

            final MessageSenderEndpoint targetSender = targetSenders.get(subscriptionCreated.getResponseChannel());
            final MessageInterceptor subscriptionInterceptor = new SubscriptionInterceptor(subscription, targetSender);
            this.registry.register(matchingSenderChannelsWith(subscription.getChannelName(), subscriptionInterceptor));

        } catch (final IllegalArgumentException e) {
            LOG.error("unable to add a subscription to channel " + subscriptionCreated.getSubscribedChannel() + ": " + e.getMessage());
            throw e;
        }
    }


    public void onSubscriptionUpdated(final SubscriptionUpdated event) {
        final Subscription subscription  = subscriptions
                .get(event.getId())
                .orElseThrow(() -> new IllegalArgumentException("Subscription " + event.getId() + " does not exist"));
        subscription.subscribe(event.getSubscribedEntities());
        subscription.unsubscribe(event.getUnsubscribedEntities());
        final SnapshotProvider snapshotProvider = snapshotProviders.get(subscription.getChannelName());
        sendSnapshot(subscription.getSubscribedEntities(), subscription.getTargetChannelName(), snapshotProvider);
    }

    public void onSubscriptionDeleted(final String subscriptionId) {
        subscriptions.remove(subscriptionId);
    }

    public Subscriptions getSubscriptions() {
        return subscriptions;
    }

    private void sendSnapshot(final Set<String> entityIds,
                              final String targetChannel,
                              final SnapshotProvider snapshotProvider) {
        final MessageSenderEndpoint messageSenderEndpoint = targetSenders.get(targetChannel);
        entityIds.forEach(id -> {
            Stream<? extends Message<?>> snapshot = snapshotProvider.snapshot(id);
            messageSenderEndpoint.sendBatch(snapshot).join();
        });
    }

}
