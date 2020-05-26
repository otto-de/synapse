package de.otto.synapse.subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.InMemoryMessageSenderFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.state.ConcurrentMapStateRepository;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.subscription.events.SubscriptionCreated;
import de.otto.synapse.subscription.events.SubscriptionUpdated;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class SubscriptionServiceTest {

    private final static String SUBSCRIBED_CHANNEL = "subscribed-channel";
    private final static String RESPONSE_CHANNEL = "response-channel";
    private final static String OTHER_RESPONSE_CHANNEL = "other-response-channel";

    private ApplicationEventPublisher eventPublisher;
    private MessageInterceptorRegistry registry;
    private MessageSenderEndpointFactory senderEndpointFactory;
    private StateRepository<String> stateRepository;
    private InMemoryChannels inMemoryChannels;

    @Before
    public void setup() {
        stateRepository = new ConcurrentMapStateRepository<>(SUBSCRIBED_CHANNEL);
        eventPublisher = mock(ApplicationEventPublisher.class);
        registry = new MessageInterceptorRegistry();
        inMemoryChannels = new InMemoryChannels(registry, eventPublisher);
        senderEndpointFactory = new InMemoryMessageSenderFactory(registry, inMemoryChannels, MessageLog.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToCreateSubscriptionForMissingSnapshotProvider() {
        final SubscriptionCreated subscriptionCreated = new SubscriptionCreated(
                "42",
                SUBSCRIBED_CHANNEL,
                RESPONSE_CHANNEL);
        final SubscriptionService service = new SubscriptionService(
                new MessageInterceptorRegistry(),
                ImmutableList.of(senderEndpointFactory),
                ImmutableList.of());
        service.onSubscriptionCreated(subscriptionCreated, MessageLog.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToCreateSubscriptionForMissingSenderEndpoint() {
        final SubscriptionCreated subscriptionCreated = new SubscriptionCreated(
                "42",
                SUBSCRIBED_CHANNEL,
                RESPONSE_CHANNEL);
        final SnapshotProvider snapshotProvider = someSnapshotProvider();
        final SubscriptionService service = new SubscriptionService(
                new MessageInterceptorRegistry(),
                ImmutableList.of(),
                ImmutableList.of(snapshotProvider));
        service.onSubscriptionCreated(subscriptionCreated, MessageLog.class);
    }

    @Test
    public void shouldRegisterInterceptorForSubscribedChannel() {
        final SubscriptionCreated subscriptionCreated = new SubscriptionCreated(
                "42",
                SUBSCRIBED_CHANNEL,
                RESPONSE_CHANNEL);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final SubscriptionService service = new SubscriptionService(
                registry,
                ImmutableList.of(senderEndpointFactory),
                ImmutableList.of(someSnapshotProvider()));
        service.onSubscriptionCreated(subscriptionCreated, MessageLog.class);
        assertThat(registry.getRegistrations(SUBSCRIBED_CHANNEL, EndpointType.SENDER), hasSize(1));
    }

    @Test
    public void shouldRegisterSeparateInterceptorsForTwoSubscriptions() {
        final SubscriptionCreated firstSubscription = new SubscriptionCreated(
                "42",
                SUBSCRIBED_CHANNEL,
                RESPONSE_CHANNEL);
        final SubscriptionCreated secondSubscription = new SubscriptionCreated(
                "4711",
                SUBSCRIBED_CHANNEL,
                RESPONSE_CHANNEL);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final SubscriptionService service = new SubscriptionService(
                registry,
                ImmutableList.of(senderEndpointFactory),
                ImmutableList.of(someSnapshotProvider()));
        service.onSubscriptionCreated(firstSubscription, MessageLog.class);
        service.onSubscriptionCreated(secondSubscription, MessageLog.class);
        assertThat(registry.getRegistrations(SUBSCRIBED_CHANNEL, EndpointType.SENDER), hasSize(2));

        final List<String> targetChannels = registry
                .getRegistrations(SUBSCRIBED_CHANNEL, EndpointType.SENDER)
                .stream()
                .map(registration -> (SubscriptionInterceptor) registration.getInterceptor())
                .map(SubscriptionInterceptor::getSubscription)
                .map(Subscription::getTargetChannelName)
                .collect(Collectors.toList());
        assertThat(targetChannels, contains(RESPONSE_CHANNEL, RESPONSE_CHANNEL));
    }

    @Test
    public void shouldSendSnapshotMessagesOnUpdatedSubscription() {
        final SubscriptionCreated subscriptionCreated = new SubscriptionCreated(
                "42",
                SUBSCRIBED_CHANNEL,
                RESPONSE_CHANNEL);
        final SubscriptionUpdated firstUpdate = new SubscriptionUpdated(
                "42",
                ImmutableSet.of("1", "2"),
                ImmutableSet.of());
        final SubscriptionUpdated secondUpdate = new SubscriptionUpdated(
                "42",
                ImmutableSet.of("3", "4"),
                ImmutableSet.of("1", "2"));

        stateRepository.put("1", "eins");
        stateRepository.put("2", "zwei");
        stateRepository.put("3", "drei");
        stateRepository.put("4", "vier");

        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final SubscriptionService service = new SubscriptionService(
                registry,
                ImmutableList.of(senderEndpointFactory),
                ImmutableList.of(someSnapshotProvider()));

        service.onSubscriptionCreated(subscriptionCreated, MessageLog.class);
        service.onSubscriptionUpdated(firstUpdate);
        service.onSubscriptionUpdated(secondUpdate);

        final Collection<String> subscribedEntities = service
                .getSubscriptions()
                .get("42")
                .orElse(null)
                .getSubscribedEntities();
        assertThat(subscribedEntities, contains("3", "4"));

        assertThat(inMemoryChannels.getChannel(RESPONSE_CHANNEL).getEventQueue(), hasSize(4));
        assertThat(inMemoryChannels.getChannel(RESPONSE_CHANNEL).getEventQueue().get(0).getKey().partitionKey(), is("1"));
        assertThat(inMemoryChannels.getChannel(RESPONSE_CHANNEL).getEventQueue().get(0).getPayload(), is("eins"));
        assertThat(inMemoryChannels.getChannel(RESPONSE_CHANNEL).getEventQueue().get(1).getKey().partitionKey(), is("2"));
        assertThat(inMemoryChannels.getChannel(RESPONSE_CHANNEL).getEventQueue().get(1).getPayload(), is("zwei"));
        assertThat(inMemoryChannels.getChannel(RESPONSE_CHANNEL).getEventQueue().get(2).getKey().partitionKey(), is("3"));
        assertThat(inMemoryChannels.getChannel(RESPONSE_CHANNEL).getEventQueue().get(2).getPayload(), is("drei"));
        assertThat(inMemoryChannels.getChannel(RESPONSE_CHANNEL).getEventQueue().get(3).getKey().partitionKey(), is("4"));
        assertThat(inMemoryChannels.getChannel(RESPONSE_CHANNEL).getEventQueue().get(3).getPayload(), is("vier"));
    }

    @Test
    public void shouldIgnoreSubscriptionCreatedForExistingSubscription() {
        final SubscriptionCreated subscriptionCreated = new SubscriptionCreated(
                "42",
                SUBSCRIBED_CHANNEL,
                RESPONSE_CHANNEL);
        final SubscriptionUpdated subscriptionUpdated = new SubscriptionUpdated(
                "42",
                ImmutableSet.of("1", "2"),
                ImmutableSet.of());

        stateRepository.put("1", "eins");
        stateRepository.put("2", "zwei");

        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final SubscriptionService service = new SubscriptionService(
                registry,
                ImmutableList.of(senderEndpointFactory),
                ImmutableList.of(someSnapshotProvider()));

        service.onSubscriptionCreated(subscriptionCreated, MessageLog.class);
        service.onSubscriptionUpdated(subscriptionUpdated);
        service.onSubscriptionCreated(subscriptionCreated, MessageLog.class);

        final Collection<String> subscribedEntities = service
                .getSubscriptions()
                .get("42")
                .orElse(null)
                .getSubscribedEntities();
        assertThat(subscribedEntities, containsInAnyOrder("1", "2"));

        assertThat(inMemoryChannels.getChannel(RESPONSE_CHANNEL).getEventQueue(), hasSize(2));
    }

    private SnapshotProvider someSnapshotProvider() {
        return new StateRepositorySnapshotProvider(SUBSCRIBED_CHANNEL, stateRepository);
    }
}
