package de.otto.synapse.edison.provider;

import de.otto.edison.testsupport.util.TestClock;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageEndpointNotification;
import de.otto.synapse.info.MessageEndpointStatus;
import de.otto.synapse.info.MessageReceiverEndpointInfo;
import org.junit.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.info.MessageEndpointStatus.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageReceiverEndpointProviderTest {

    private final MessageReceiverEndpointInfoProvider provider;
    private TestClock testClock = TestClock.now();
    private List<EventSource> eventSources = new ArrayList<EventSource>() {{
        final EventSource foo = mock(EventSource.class);
        when(foo.getChannelName()).thenReturn("foo");
        final EventSource bar = mock(EventSource.class);
        when(bar.getChannelName()).thenReturn("bar");

        add(foo);
        add(bar);
    }};

    public MessageReceiverEndpointProviderTest() {
        provider = new MessageReceiverEndpointInfoProvider(Optional.of(eventSources), testClock);
    }

    @Test
    public void shouldReturnInfoForStartingEndpoint() {
        //given
        MessageEndpointNotification messageEndpointNotification = createEventSourceNotification("foo", STARTING, "Loading snapshot");

        //when
        provider.onEventSourceNotification(messageEndpointNotification);

        //then
        MessageReceiverEndpointInfo info = provider.getInfos().getChannelInfoFor("foo");

        assertThat(info.getChannelName(), is("foo"));
        assertThat(info.getStatus(), is(STARTING));
        assertThat(info.getMessage(), is("Loading snapshot"));
    }


    @Test
    public void shouldReturnInfoForTwoStartingEndpoints() {
        //given
        MessageEndpointNotification firstMessageEndpointNotification = createEventSourceNotification("foo", STARTING, "Loading snapshot");
        MessageEndpointNotification secondMessageEndpointNotification = createEventSourceNotification("bar", STARTING, "Loading snapshot");

        //when
        provider.onEventSourceNotification(firstMessageEndpointNotification);
        provider.onEventSourceNotification(secondMessageEndpointNotification);

        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        assertThat(fooInfo.getChannelName(), is("foo"));
        assertThat(fooInfo.getStatus(), is(STARTING));
        assertThat(fooInfo.getMessage(), is("Loading snapshot"));

        MessageReceiverEndpointInfo barInfo = provider.getInfos().getChannelInfoFor("bar");

        assertThat(barInfo.getChannelName(), is("bar"));
        assertThat(barInfo.getStatus(), is(STARTING));
        assertThat(barInfo.getMessage(), is("Loading snapshot"));
    }

    @Test
    public void shouldReturnInfoForFinishedEndpoints() {
        //given

        //when
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTING, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTED, "Loading snapshot"));
        testClock.proceed(2, ChronoUnit.SECONDS);
        provider.onEventSourceNotification(createEventSourceNotification("bar", STARTING, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotification("bar", STARTED, "Loading snapshot"));
        testClock.proceed(2, ChronoUnit.SECONDS);
        provider.onEventSourceNotification(createEventSourceNotification("foo", FINISHED, "Done."));
        testClock.proceed(3, ChronoUnit.SECONDS);
        provider.onEventSourceNotification(createEventSourceNotification("bar", FINISHED, "Done."));

        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        assertThat(fooInfo.getChannelName(), is("foo"));
        assertThat(fooInfo.getStatus(), is(FINISHED));
        assertThat(fooInfo.getMessage(), is("Done. Finished consumption after PT4S."));

        MessageReceiverEndpointInfo barInfo = provider.getInfos().getChannelInfoFor("bar");

        assertThat(barInfo.getChannelName(), is("bar"));
        assertThat(barInfo.getStatus(), is(FINISHED));
        assertThat(barInfo.getMessage(), is("Done. Finished consumption after PT5S."));
    }

    @Test
    public void shouldReturnInfoForFailedEndpoints() {
        //given

        //when
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTING, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTED, "Loading snapshot"));
        testClock.proceed(2, ChronoUnit.SECONDS);
        provider.onEventSourceNotification(createEventSourceNotification("foo", FAILED, "Kawumm!!!"));

        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        assertThat(fooInfo.getChannelName(), is("foo"));
        assertThat(fooInfo.getStatus(), is(FAILED));
        assertThat(fooInfo.getMessage(), is("Kawumm!!!"));
    }

    @Test
    public void shouldDisplayDurationBehindForSingleShard() {
        //given

        //when
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTING, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTED, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotification("foo", RUNNING, "Done."));


        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        // TODO
        assertThat(fooInfo.getChannelPosition().get().getDurationBehind(), is(Duration.ofHours(1)));
    }

    @Test
    public void shouldKeepDurationBehindForFinishedChannels() {
        //given

        //when
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTING, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTED, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotification("foo", RUNNING, "Running."));
        provider.onEventSourceNotification(createEventSourceNotification("foo", FINISHED, "Done."));


        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        // TODO
        assertThat(fooInfo.getChannelPosition().get().getDurationBehind(), is(Duration.ofHours(1)));
    }

    @Test
    public void shouldDisplayDurationBehindForMultipleShards() {
        //given

        //when
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTING, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTED, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotificationBuilder("foo", RUNNING, "running...","first", 2 ).build());
        provider.onEventSourceNotification(createEventSourceNotificationBuilder("foo", RUNNING, "running...","second", 1 ).build());

        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        assertThat(fooInfo.getChannelPosition().get().getDurationBehind(), is(Duration.ofHours(2)));
    }

    @Test
    public void shouldDisplayDurationBehindForMultipleShardsAndMultipleRunningEvents() {
        //given

        //when
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTING, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotification("foo", STARTED, "Loading snapshot"));
        provider.onEventSourceNotification(createEventSourceNotificationBuilder("foo", RUNNING, "running...","first", 3 ).build());
        provider.onEventSourceNotification(createEventSourceNotificationBuilder("foo", RUNNING, "running...","first", 2 ).build());
        provider.onEventSourceNotification(createEventSourceNotificationBuilder("foo", RUNNING, "running...","second", 4 ).build());
        provider.onEventSourceNotification(createEventSourceNotificationBuilder("foo", RUNNING, "running...","second", 1 ).build());

        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        assertThat(fooInfo.getChannelPosition().get().getDurationBehind(), is(Duration.ofHours(2)));
    }

    private MessageEndpointNotification createEventSourceNotification(final String channelName,
                                                                      final MessageEndpointStatus status,
                                                                      final String message) {
        return createEventSourceNotificationBuilder(channelName, status, message).build();
    }

    private MessageEndpointNotification.Builder createEventSourceNotificationBuilder(final String channelName,
                                                                                     final MessageEndpointStatus status,
                                                                                     final String message) {
        MessageEndpointNotification.Builder builder = MessageEndpointNotification.builder()
                .withStatus(status)
                .withEventSourceName("snapshot")
                .withChannelName(channelName)
                .withMessage(message);
        if (status == RUNNING) {
            builder.withChannelPosition(channelPosition(fromPosition("single-shard", Duration.ofHours(1), "42")));
        }
        return builder;
    }

    private MessageEndpointNotification.Builder createEventSourceNotificationBuilder(final String channelName,
                                                                                     final MessageEndpointStatus status,
                                                                                     final String message,
                                                                                     final String shardName,
                                                                                     final int hours) {
        return MessageEndpointNotification.builder()
                .withStatus(status)
                .withEventSourceName("snapshot")
                .withChannelName(channelName)
                .withMessage(message)
                .withChannelPosition(channelPosition(fromPosition(shardName, Duration.ofHours(hours), "42")));
    }

}