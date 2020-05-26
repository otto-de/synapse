package de.otto.synapse.subscription;

import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.Sets.newConcurrentHashSet;

public class Subscription {
    private final String id;
    private final String channelName;
    private final String targetChannelName;
    private final Set<String> subscribedEntities;

    public Subscription(final String id,
                        final String channelName,
                        final String targetChannelName) {
        this.id = id;
        this.channelName = channelName;
        this.targetChannelName = targetChannelName;
        this.subscribedEntities = newConcurrentHashSet();
    }

    public String getId() {
        return id;
    }

    public String getChannelName() {
        return channelName;
    }

    public String getTargetChannelName() {
        return targetChannelName;
    }

    public Set<String> getSubscribedEntities() {
        return subscribedEntities;
    }

    public void subscribe(final Set<String> subscribedEntities) {
        this.subscribedEntities.addAll(subscribedEntities);
    }

    public void unsubscribe(final Set<String> unsubscribedEntities) {
        this.subscribedEntities.removeAll(unsubscribedEntities);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Subscription)) return false;
        Subscription that = (Subscription) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(channelName, that.channelName) &&
                Objects.equals(targetChannelName, that.targetChannelName) &&
                Objects.equals(subscribedEntities, that.subscribedEntities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, channelName, targetChannelName, subscribedEntities);
    }

    @Override
    public String toString() {
        return "Subscription{" +
                "id='" + id + '\'' +
                ", channelName='" + channelName + '\'' +
                ", targetChannelName='" + targetChannelName + '\'' +
                ", subscribedEntities=" + subscribedEntities +
                '}';
    }
}
