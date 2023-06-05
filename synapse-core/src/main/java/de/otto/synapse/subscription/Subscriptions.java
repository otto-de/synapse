package de.otto.synapse.subscription;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.collect.Maps.newConcurrentMap;
import static org.slf4j.LoggerFactory.getLogger;


public class Subscriptions {

    private static final Logger LOG = getLogger(Subscriptions.class);

    final ConcurrentMap<String, Map<String, Subscription>> subscriptions = newConcurrentMap();

    public void addIfMissing(final Subscription subscription) {
        final String channelName = subscription.getChannelName();
        LOG.info("Received subscription for channel " + channelName);
        subscriptions.putIfAbsent(channelName, Maps.newConcurrentMap());
        subscriptions.get(channelName).putIfAbsent(subscription.getId(), subscription);
    }

    public void subscribe(final String subscriptionId, final Set<String> subscribedEntities) {
        get(subscriptionId).orElseThrow(() -> new IllegalArgumentException("Subscription does not exist")).subscribe(subscribedEntities);
    }

    public void unsubscribe(final String subscriptionId, final Set<String> unsubscribedEntities) {
        get(subscriptionId).ifPresent(subscription -> subscription.unsubscribe(unsubscribedEntities));
    }

    public Collection<Subscription> subscriptionsFor(final String channelName) {
        return subscriptions.getOrDefault(channelName, ImmutableMap.of()).values();
    }

    public Optional<Subscription> get(final String subscriptionId) {
        for (Map<String, Subscription> s : subscriptions.values()) {
            final Optional<Subscription> optionalSubscription = s.values()
                    .stream()
                    .filter(subscription -> subscription.getId().equals(subscriptionId))
                    .findAny();
            if (optionalSubscription.isPresent()) {
                return optionalSubscription;
            }
        }
        return Optional.empty();
    }

    public void remove(final String subscriptionId) {
        LOG.info("Removed subscription " + subscriptionId);
        subscriptions.values().forEach(map -> map.remove(subscriptionId));
    }
}
