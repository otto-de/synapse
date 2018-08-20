package de.otto.synapse.edison.logging;

import de.otto.edison.testsupport.util.TestClock;
import de.otto.synapse.edison.logging.EventSourceSyncDurationLogger;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertThat;

public class EventSourceSyncDurationLoggerTest {

    private EventSourceSyncDurationLogger eventSourceSyncDurationLogger;
    private TestClock testClock;

    @Before
    public void setUp() {
        testClock = TestClock.now();
        eventSourceSyncDurationLogger = new EventSourceSyncDurationLogger();
        eventSourceSyncDurationLogger.setClock(testClock);
    }

    @Test
    public void shouldPrintEventSourceDuration() {
        //when
        startingEvent("channelA");
        testClock.proceed(5, MINUTES);
        testClock.proceed(2, SECONDS);
        String message = inSyncEvent("channelA");

        //then
        assertThat(message, is("KinesisEventSource 'channelA' duration for getting in sync : PT5M2S"));
    }


    @Test
    public void shouldPrintEventSourceDurationForMultipleChannels() {
        //when
        startingEvent("channelA");
        testClock.proceed(1, MINUTES);
        startingEvent("channelB");
        testClock.proceed(5, MINUTES);
        String messageA = inSyncEvent("channelA");
        String messageB = inSyncEvent("channelB");

        //then
        assertThat(messageA, is("KinesisEventSource 'channelA' duration for getting in sync : PT6M"));
        assertThat(messageB, is("KinesisEventSource 'channelB' duration for getting in sync : PT5M"));
    }

    @Test
    public void shouldPrintEventSourceDurationForMultipleChannelsAndIgnoreFutureNotifications() {
        shouldPrintEventSourceDurationForMultipleChannels();

        String message = inSyncEvent("channelB");

        assertThat(message, isEmptyString());
    }

    private String inSyncEvent(String channelName) {
        return this.eventSourceSyncDurationLogger.on(MessageReceiverNotification.builder()
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