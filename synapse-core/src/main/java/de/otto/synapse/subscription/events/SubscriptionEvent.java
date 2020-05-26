package de.otto.synapse.subscription.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;


@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SubscriptionCreated.class, name = "CREATED"),
        @JsonSubTypes.Type(value = SubscriptionUpdated.class, name = "UPDATED")})
public abstract class SubscriptionEvent {

    public enum Type { CREATED, UPDATED}

    private final String id;
    private final Type type;

    public SubscriptionEvent(final String id,
                             final Type type) {
        this.id = id;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public Type getType() {
        return type;
    }

    public abstract SubscriptionCreated asSubscriptionCreated();
    public abstract SubscriptionUpdated asSubscriptionUpdated();

    @Override
    public String toString() {
        return "SubscriptionEvent{" +
                "id='" + id + '\'' +
                ", type=" + type +
                '}';
    }
}
