package de.otto.synapse.subscription;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class SubscriptionsTest {

    @Test
    public void shouldAddSubscription() {
        final Subscriptions subscriptions = new Subscriptions();
        final Subscription subscription = new Subscription("foo", "foo-channel", "bar-channel");
        subscriptions.addIfMissing(subscription);
        assertThat(subscriptions.get("foo").isPresent(), is(true));
        assertThat(subscriptions.get("foo").get(), is(subscription));
    }

    @Test
    public void shouldAddSubscriptionOnlyIfMissing() {
        final Subscriptions subscriptions = new Subscriptions();
        final Subscription subscription = new Subscription("foo", "foo-channel", "bar-channel");
        subscriptions.addIfMissing(subscription);
        subscriptions.addIfMissing(subscription);
        assertThat(subscriptions.subscriptionsFor("foo-channel"), hasSize(1));
    }

    @Test
    public void shouldRemoveSubscription() {
        final Subscriptions subscriptions = new Subscriptions();
        final Subscription subscription = new Subscription("foo", "foo-channel", "bar-channel");
        subscription.subscribe(ImmutableSet.of("1", "2"));
        subscriptions.addIfMissing(subscription);
        subscriptions.remove("foo");
        assertThat(subscriptions.get("foo").isPresent(), is(false));
    }

    @Test
    public void shouldSubscribe() {
        final Subscriptions subscriptions = new Subscriptions();
        final Subscription subscription = new Subscription("foo", "foo-channel", "bar-channel");
        subscriptions.addIfMissing(subscription);
        subscriptions.subscribe("foo", ImmutableSet.of("1", "2"));
        subscriptions.subscribe("foo", ImmutableSet.of("3"));
        assertThat(subscriptions.get("foo").get().getSubscribedEntities(), containsInAnyOrder("1", "2", "3"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToSubscribeForMissingSubscription() {
        final Subscriptions subscriptions = new Subscriptions();
        subscriptions.subscribe("foo", ImmutableSet.of("1"));
    }

    @Test
    public void shouldUnsubscribe() {
        final Subscriptions subscriptions = new Subscriptions();
        final Subscription subscription = new Subscription("foo", "foo-channel", "bar-channel");
        subscription.subscribe(ImmutableSet.of("1", "2"));
        subscriptions.addIfMissing(subscription);
        subscriptions.unsubscribe("foo", ImmutableSet.of("1"));
        assertThat(subscriptions.get("foo").get().getSubscribedEntities(), contains("2"));
    }

}