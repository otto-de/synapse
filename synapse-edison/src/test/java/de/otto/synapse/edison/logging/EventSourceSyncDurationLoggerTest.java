package de.otto.synapse.edison.logging;

import de.otto.edison.testsupport.util.TestClock;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.of;
import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventSourceSyncDurationLoggerTest {

    private EventSourceSyncDurationLogger eventSourceSyncDurationLogger;
    private TestClock testClock;
    private List<String> logMessages = new ArrayList<>();


    @Before
    public void setUp() {
        logMessages.clear();
        testClock = TestClock.now();
        EventSource eventSourceA = withMockEventSource("channelA");
        EventSource eventSourceB = withMockEventSource("channelB");
        Optional<List<EventSource>> optionalList = Optional.of(of(eventSourceA, eventSourceB));
        eventSourceSyncDurationLogger = new EventSourceSyncDurationLogger(optionalList) {
            @Override
            void log(String message) {
                logMessages.add(message);
            }
        };
        eventSourceSyncDurationLogger.setClock(testClock);
    }

    private EventSource withMockEventSource(String channelName) {
        EventSource eventSource = mock(EventSource.class);
        when(eventSource.getChannelName()).thenReturn(channelName);
        return eventSource;
    }

    @Test
    public void shouldPrintEventSourceDuration() {
        //when
        startingEvent("channelA");
        testClock.proceed(5, MINUTES);
        testClock.proceed(2, SECONDS);
        inSyncEvent("channelA");

        //then
        assertThat(logMessages.get(0), is("EventSource 'channelA' duration for getting in sync: PT5M2S"));
    }


    @Test
    public void shouldPrintEventSourceDurationForMultipleChannels() {
        //when
        startingEvent("channelA");
        testClock.proceed(1, MINUTES);
        startingEvent("channelB");
        testClock.proceed(5, MINUTES);
        inSyncEvent("channelA");
        inSyncEvent("channelB");

        //then
        assertThat(logMessages, hasSize(3));
        assertThat(logMessages.get(0), is("EventSource 'channelA' duration for getting in sync: PT6M"));
        assertThat(logMessages.get(1), is("EventSource 'channelB' duration for getting in sync: PT5M"));
        assertThat(logMessages.get(2), is("All channels up to date after PT6M"));


    }

    @Test
    public void shouldPrintEventSourceDurationForMultipleChannelsAndIgnoreFutureNotifications() {
        shouldPrintEventSourceDurationForMultipleChannels();
        logMessages.clear();

        inSyncEvent("channelB");

        assertThat(logMessages, hasSize(0));
    }


    @Test
    public void shouldIgnoreNotificationsOnMissingEventSources() {
        // given
        eventSourceSyncDurationLogger = new EventSourceSyncDurationLogger(Optional.empty());

        // when
        startingEvent("unknown");
        testClock.proceed(1, MINUTES);
        inSyncEvent("unknown");

        // then
        assertThat(logMessages, hasSize(0));
    }


    private void inSyncEvent(String channelName) {
        this.eventSourceSyncDurationLogger.on(MessageReceiverNotification.builder()
                .withStatus(MessageReceiverStatus.RUNNING)
                .withChannelDurationBehind(channelDurationBehind().with("shard1", Duration.ofSeconds(1)).build())
                .withChannelName(channelName)
                .build());
    }

    private void startingEvent(String channelName) {
        eventSourceSyncDurationLogger.on(MessageReceiverNotification.builder()
                .withStatus(MessageReceiverStatus.STARTING)
                .withChannelName(channelName)
                .build());
    }


}
