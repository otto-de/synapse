package de.otto.synapse.subscription.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class SubscriptionUpdated extends SubscriptionEvent {
    private final Set<String> subscribedEntities;
    private final Set<String> unsubscribedEntities;

    public SubscriptionUpdated(final @JsonProperty("id") String id,
                               final @JsonProperty("subscribedEntities") Set<String> subscribedEntities,
                               final @JsonProperty("unsubscribedEntities") Set<String> unsubscribedEntities) {
        super(id, Type.UPDATED);
        this.subscribedEntities = subscribedEntities;
        this.unsubscribedEntities = unsubscribedEntities;
    }

    @Override
    public SubscriptionCreated asSubscriptionCreated() {
        throw new ClassCastException("not a SubscriptionCreated event");
    }

    @Override
    public SubscriptionUpdated asSubscriptionUpdated() {
        return this;
    }

    public Set<String> getSubscribedEntities() {
        return subscribedEntities != null ? subscribedEntities : ImmutableSet.of();
    }

    public Set<String> getUnsubscribedEntities() {
        return unsubscribedEntities != null ? unsubscribedEntities : ImmutableSet.of();
    }

    @Override
    public String toString() {
        return "SubscriptionUpdated{" +
                "subscribedEntities=" + subscribedEntities +
                ", unsubscribedEntities=" + unsubscribedEntities +
                '}';
    }
}
