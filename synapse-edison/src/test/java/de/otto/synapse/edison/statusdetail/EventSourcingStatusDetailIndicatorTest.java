package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.testsupport.util.TestClock;
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
        EventSourceNotification eventSourceNotification = createEventSourceNotification("myChannelName", STARTED, "Loading snapshot");

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
        EventSourceNotification firstEventSourceNotification = createEventSourceNotification("myChannelName", STARTED, "Loading snapshot");
        EventSourceNotification secondEventSourceNotification = createEventSourceNotification("myChannelName2", STARTED, "Loading snapshot");

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
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createEventSourceNotification("myChannelName", STARTED, "Loading snapshot"));
        testClock.proceed(2, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createEventSourceNotification("myChannelName2", STARTED, "Loading snapshot"));
        testClock.proceed(2, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createEventSourceNotification("myChannelName", FINISHED, "Done."));
        testClock.proceed(3, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createEventSourceNotification("myChannelName2", FINISHED, "Done."));

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
    public void shouldDisplayDurationBehindForSingleShard() {
        //given

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createEventSourceNotification("myChannelName", STARTED, "Loading snapshot"));
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createEventSourceNotification("myChannelName", RUNNING, "Done."));

        //then
        List<StatusDetail> statusDetails = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(0).getName(), is("myChannelName"));
        assertThat(statusDetails.get(0).getMessage(), is("Channel is PT1H behind head."));
    }

    @Test
    public void shouldDisplayDurationBehindForMultipleShards() {
        //given

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createEventSourceNotification("myChannelName", STARTED, "Loading snapshot"));

        eventSourcingStatusDetailIndicator.onEventSourceNotification(createEventSourceNotificationBuilder("myChannelName", RUNNING, "Done.")
                .withChannelPosition(channelPosition(fromPosition("shard1", Duration.ofHours(1), "pos1"))).build());
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createEventSourceNotificationBuilder("myChannelName", RUNNING, "Done.")
                .withChannelPosition(channelPosition(fromPosition("shard2", Duration.ofHours(2), "pos2"))).build());
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createEventSourceNotificationBuilder("myChannelName", RUNNING, "Done.")
                .withChannelPosition(channelPosition(fromPosition("shard3", Duration.ofHours(0), "pos3"))).build());

        //then
        List<StatusDetail> statusDetails = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(0).getName(), is("myChannelName"));
        assertThat(statusDetails.get(0).getMessage(), is("Channel is PT2H behind head."));
    }

    private EventSourceNotification createEventSourceNotification(final String channelName,
                                                                  final EventSourceNotification.Status status,
                                                                  final String message) {
        return createEventSourceNotificationBuilder(channelName, status, message).build();
    }

    private EventSourceNotification.Builder createEventSourceNotificationBuilder(final String channelName,
                                                                          final EventSourceNotification.Status status,
                                                                          final String message) {
        return EventSourceNotification.builder()
                .withStatus(status)
                .withEventSourceName("snapshot")
                .withChannelName(channelName)
                .withMessage(message)
                .withChannelPosition(channelPosition(fromPosition("single-shard", Duration.ofHours(1), "42")));
    }

}