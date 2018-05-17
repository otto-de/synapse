package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.testsupport.util.TestClock;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.eventsource.EventSourceNotification;
import org.junit.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.eventsource.EventSourceNotification.Status.FINISHED;
import static de.otto.synapse.eventsource.EventSourceNotification.Status.RUNNING;
import static de.otto.synapse.eventsource.EventSourceNotification.Status.STARTED;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EventSourcingStatusDetailIndicatorTest {

    private final EventSourcingStatusDetailIndicator eventSourcingStatusDetailIndicator;
    private TestClock testClock = TestClock.now();

    public EventSourcingStatusDetailIndicatorTest() {
        eventSourcingStatusDetailIndicator = new EventSourcingStatusDetailIndicator(testClock);
    }

    @Test
    public void shouldUpdateStatusDetailForSnapshotStarted() {
        //given
        EventSourceNotification eventSourceNotification = createSnapshotEventSourceNotification("myChannelName", STARTED, "Loading snapshot");

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(eventSourceNotification);

        //then
        List<StatusDetail> statusDetail = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetail.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetail.get(0).getName(), is("myChannelName"));
        assertThat(statusDetail.get(0).getMessage(), is("Loading snapshot"));
    }


    @Test
    public void shouldUpdateStatusDetailFor2ndSnapshotStarted() {
        //given
        EventSourceNotification firstEventSourceNotification = createSnapshotEventSourceNotification("myChannelName", STARTED, "Loading snapshot");
        EventSourceNotification secondEventSourceNotification = createSnapshotEventSourceNotification("myChannelName2", STARTED, "Loading snapshot");

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(firstEventSourceNotification);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(secondEventSourceNotification);

        //then
        List<StatusDetail> statusDetails = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(0).getName(), is("myChannelName"));
        assertThat(statusDetails.get(0).getMessage(), is("Loading snapshot"));
        assertThat(statusDetails.get(1).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(1).getName(), is("myChannelName2"));
        assertThat(statusDetails.get(1).getMessage(), is("Loading snapshot"));
    }

    @Test
    public void shouldCalculateStartupTimes() {
        //given

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myChannelName", STARTED, "Loading snapshot"));
        testClock.proceed(2, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myChannelName2", STARTED, "Loading snapshot"));
        testClock.proceed(2, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myChannelName", FINISHED, "Done."));
        testClock.proceed(3, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myChannelName2", FINISHED, "Done."));

        //then
        List<StatusDetail> statusDetails = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(0).getName(), is("myChannelName"));
        assertThat(statusDetails.get(0).getMessage(), is("Done. Finished consumption after PT4S."));
        assertThat(statusDetails.get(1).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(1).getName(), is("myChannelName2"));
        assertThat(statusDetails.get(1).getMessage(), is("Done. Finished consumption after PT5S."));
    }

    @Test
    public void shouldDisplayDurationBehindPerShard() {
        //given

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myChannelName", STARTED, "Loading snapshot"));
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myChannelName", RUNNING, "Done."));

        //then
        List<StatusDetail> statusDetails = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(0).getName(), is("myChannelName"));
        assertThat(statusDetails.get(0).getMessage(), is("Channel is PT1H behind head."));
    }

    private EventSourceNotification createSnapshotEventSourceNotification(final String channelName,
                                                                          final EventSourceNotification.Status status,
                                                                          final String message) {
        return EventSourceNotification.builder()
                .withStatus(status)
                .withEventSourceName("snapshot")
                .withChannelName(channelName)
                .withMessage(message)
                .withChannelPosition(channelPosition(fromPosition("single-shard", Duration.ofHours(1), "42")))
                .build();
    }

}