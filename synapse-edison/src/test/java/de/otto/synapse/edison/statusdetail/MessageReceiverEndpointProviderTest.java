package de.otto.synapse.edison.statusdetail;

import de.otto.edison.testsupport.util.TestClock;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageReceiverEndpointInfo;
import de.otto.synapse.info.MessageReceiverEndpointInfoProvider;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.*;
import static java.time.Duration.ofHours;
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
        MessageReceiverNotification messageEndpointNotification = notificationOf("foo", STARTING, "Loading snapshot").build();

        //when
        provider.on(messageEndpointNotification);

        //then
        MessageReceiverEndpointInfo info = provider.getInfos().getChannelInfoFor("foo");

        assertThat(info.getChannelName(), is("foo"));
        assertThat(info.getStatus(), is(STARTING));
        assertThat(info.getMessage(), is("Loading snapshot"));
    }


    @Test
    public void shouldReturnInfoForTwoStartingEndpoints() {
        //given
        MessageReceiverNotification firstMessageEndpointNotification = notificationOf("foo", STARTING, "Loading snapshot").build();
        MessageReceiverNotification secondMessageEndpointNotification = notificationOf("bar", STARTING, "Loading snapshot").build();

        //when
        provider.on(firstMessageEndpointNotification);
        provider.on(secondMessageEndpointNotification);

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
        provider.on(notificationOf("foo", STARTING, "Loading snapshot").build());
        provider.on(notificationOf("foo", STARTED, "Loading snapshot").build());
        testClock.proceed(2, ChronoUnit.SECONDS);
        provider.on(notificationOf("bar", STARTING, "Loading snapshot").build());
        provider.on(notificationOf("bar", STARTED, "Loading snapshot").build());
        testClock.proceed(2, ChronoUnit.SECONDS);
        provider.on(notificationOf("foo", FINISHED, "Done.").build());
        testClock.proceed(3, ChronoUnit.SECONDS);
        provider.on(notificationOf("bar", FINISHED, "Done.").build());

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
        provider.on(notificationOf("foo", STARTING, "Loading snapshot").build());
        provider.on(notificationOf("foo", STARTED, "Loading snapshot").build());
        testClock.proceed(2, ChronoUnit.SECONDS);
        provider.on(notificationOf("foo", FAILED, "Kawumm!!!").build());

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
        provider.on(notificationOf("foo", STARTING, "Loading snapshot").build());
        provider.on(notificationOf("foo", STARTED, "Loading snapshot").build());
        provider.on(notificationOf("foo", RUNNING, "Done.").build());


        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        assertThat(fooInfo.getDurationBehind().get().getDurationBehind(), is(ofHours(2)));
    }

    @Test
    public void shouldKeepDurationBehindForFinishedChannels() {
        //given

        //when
        provider.on(notificationOf("foo", STARTING, "Loading snapshot").build());
        provider.on(notificationOf("foo", STARTED, "Loading snapshot").build());
        provider.on(notificationOf("foo", RUNNING, "Running.").build());
        provider.on(notificationOf("foo", FINISHED, "Done.").build());


        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        assertThat(fooInfo.getDurationBehind().get().getDurationBehind(), is(ofHours(2)));
    }

    @Test
    public void shouldDisplayDurationBehindForMultipleShards() {
        //given

        //when
        provider.on(notificationOf("foo", STARTING, "Loading snapshot").build());
        provider.on(notificationOf("foo", STARTED, "Loading snapshot").build());
        provider.on(notificationOf("foo", RUNNING, "running...",2, 1 ).build());

        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        assertThat(fooInfo.getDurationBehind().get().getDurationBehind(), is(ofHours(2)));
    }

    @Test
    public void shouldDisplayDurationBehindForMultipleShardsAndMultipleRunningEvents() {
        //given

        //when
        provider.on(notificationOf("foo", STARTING, "Loading snapshot").build());
        provider.on(notificationOf("foo", STARTED, "Loading snapshot").build());
        provider.on(notificationOf("foo", RUNNING, "running...",5L, 3L).build());
        provider.on(notificationOf("foo", RUNNING, "running...",4L, 2L).build());
        provider.on(notificationOf("foo", RUNNING, "running...",4L, 1L).build());
        provider.on(notificationOf("foo", RUNNING, "running...",1L, 0L).build());

        //then
        MessageReceiverEndpointInfo fooInfo = provider.getInfos().getChannelInfoFor("foo");

        assertThat(fooInfo.getDurationBehind().get().getDurationBehind(), is(ofHours(1)));
        assertThat(fooInfo.getDurationBehind().get(), is(channelDurationBehind()
                .with("first-shard", ofHours(1L))
                .with("second-shard", ofHours(0L))
                .build())
        );
    }

    private MessageReceiverNotification.Builder notificationOf(final String channelName,
                                                               final MessageReceiverStatus status,
                                                               final String message) {
        final MessageReceiverNotification.Builder builder = builder()
                .withStatus(status)
                .withChannelName(channelName)
                .withMessage(message);
        if (status == RUNNING) {
            builder.withChannelDurationBehind(channelDurationBehind()
                    .with("first-shard", ofHours(1))
                    .with("second-shard", ofHours(2))
                    .build());
        }
        return builder;
    }

    private MessageReceiverNotification.Builder notificationOf(final String channelName,
                                                               final MessageReceiverStatus status,
                                                               final String message,
                                                               final long firstShardBehind,
                                                               final long secondShardBehind) {
        final MessageReceiverNotification.Builder builder = builder()
                .withStatus(status)
                .withChannelName(channelName)
                .withMessage(message);
        if (status == RUNNING) {
            builder.withChannelDurationBehind(
                    channelDurationBehind()
                            .with("first-shard", ofHours(firstShardBehind))
                            .with("second-shard", ofHours(secondShardBehind))
                            .build());
        }
        return builder;
    }

}