package de.otto.synapse.subscription.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SubscriptionCreated extends SubscriptionEvent {
    private final String subscribedChannel;
    private final String responseChannel;

    @JsonCreator
    public SubscriptionCreated(final @JsonProperty("id") String id,
                               final @JsonProperty("subscribedChannel") String subscribedChannel,
                               final @JsonProperty("responseChannel") String responseChannel) {
        super(id, Type.CREATED);
        this.subscribedChannel = subscribedChannel;
        this.responseChannel = responseChannel;
    }

    @Override
    public SubscriptionCreated asSubscriptionCreated() {
        return this;
    }

    @Override
    public SubscriptionUpdated asSubscriptionUpdated() {
        throw new ClassCastException("not a SubscriptionUpdated event");
    }

    public String getSubscribedChannel() {
        return subscribedChannel;
    }

    public String getResponseChannel() {
        return responseChannel;
    }

    @Override
    public String toString() {
        return "SubscriptionCreated{" +
                "subscribedChannel='" + subscribedChannel + '\'' +
                ", responseChannel='" + responseChannel + '\'' +
                '}';
    }
}
