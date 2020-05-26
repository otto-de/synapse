package de.otto.synapse.subscription.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static de.otto.synapse.translator.ObjectMappers.defaultObjectMapper;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class SubscriptionEventTest {

    @Test
    public void shouldSerializeSubscriptionCreated() throws JsonProcessingException {
        final SubscriptionEvent subscriptionCreated = new SubscriptionCreated(
                "42",
                "subscribed-channel",
                "response-channel");
        final String json = defaultObjectMapper().writeValueAsString(subscriptionCreated);
        final String expectedJson =
                "{" +
                "\"id\":\"42\"," +
                "\"subscribedChannel\":\"subscribed-channel\"," +
                "\"responseChannel\":\"response-channel\"," +
                "\"type\":\"CREATED\"" +
                "}";
        assertThat(json, is(expectedJson));
    }

    @Test
    public void shouldDeserializeAsSubscriptionCreated() throws JsonProcessingException {
        final String json =
                "{\n" +
                "   \"id\": \"42\",\n" +
                "   \"type\": \"CREATED\",\n" +
                "   \"subscribedChannel\": \"subscribed-channel\",\n" +
                "   \"responseChannel\": \"response-channel\"\n" +
                "}";

        final SubscriptionEvent event = defaultObjectMapper().readValue(json, SubscriptionEvent.class);
        assertThat(event, is(instanceOf(SubscriptionCreated.class)));

        final SubscriptionCreated subscriptionCreated = event.asSubscriptionCreated();
        assertThat(subscriptionCreated.getId(), is("42"));
        assertThat(subscriptionCreated.getType(), is(SubscriptionEvent.Type.CREATED));
        assertThat(subscriptionCreated.getSubscribedChannel(), is("subscribed-channel"));
        assertThat(subscriptionCreated.getResponseChannel(), is("response-channel"));
    }

    @Test
    public void shouldSerializeSubscriptionUpdated() throws JsonProcessingException {
        final SubscriptionEvent subscriptionUpdated = new SubscriptionUpdated(
                "42",
                ImmutableSet.of("3", "4"),
                ImmutableSet.of("1", "2"));
        final String json = defaultObjectMapper().writeValueAsString(subscriptionUpdated);
        final String expectedJson =
                "{" +
                        "\"id\":\"42\"," +
                        "\"subscribedEntities\":[\"3\",\"4\"]," +
                        "\"unsubscribedEntities\":[\"1\",\"2\"]," +
                        "\"type\":\"UPDATED\"" +
                        "}";
        assertThat(json, is(expectedJson));
    }

    @Test
    public void shouldDeserializeAsSubscriptionUpdated() throws JsonProcessingException {
        final String json =
                "{\n" +
                "   \"id\": \"42\",\n" +
                "   \"type\": \"UPDATED\",\n" +
                "   \"subscribedEntities\": [\"3\", \"4\"],\n" +
                "   \"unsubscribedEntities\": [\"1\", \"2\"]\n" +
                "}";

        final SubscriptionEvent event = defaultObjectMapper().readValue(json, SubscriptionEvent.class);
        assertThat(event, is(instanceOf(SubscriptionUpdated.class)));

        final SubscriptionUpdated subscriptionUpdated = event.asSubscriptionUpdated();
        assertThat(subscriptionUpdated.getId(), is("42"));
        assertThat(subscriptionUpdated.getType(), is(SubscriptionEvent.Type.UPDATED));
        assertThat(subscriptionUpdated.getSubscribedEntities(), contains("3", "4"));
        assertThat(subscriptionUpdated.getUnsubscribedEntities(), contains("1", "2"));
    }
}