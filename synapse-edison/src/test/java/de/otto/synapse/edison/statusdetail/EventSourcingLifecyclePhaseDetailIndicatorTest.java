package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.testsupport.util.TestClock;
import de.otto.synapse.eventsource.EventSourceNotification;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.List;

import static de.otto.synapse.eventsource.EventSourceNotification.Status.FINISHED;
import static de.otto.synapse.eventsource.EventSourceNotification.Status.STARTED;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EventSourcingLifecyclePhaseDetailIndicatorTest {

    private final EventSourcingStatusDetailIndicator eventSourcingStatusDetailIndicator;
    private TestClock testClock = TestClock.now();

    public EventSourcingLifecyclePhaseDetailIndicatorTest() {
        eventSourcingStatusDetailIndicator = new EventSourcingStatusDetailIndicator(testClock);
    }

    @Test
    public void shouldUpdateStatusDetailForSnapshotStarted() {
        //given
        EventSourceNotification eventSourceNotification = createSnapshotEventSourceNotification("myStreamName", STARTED, "Loading snapshot");

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(eventSourceNotification);

        //then
        List<StatusDetail> statusDetail = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetail.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetail.get(0).getName(), is("myStreamName"));
        assertThat(statusDetail.get(0).getMessage(), is("Loading snapshot"));
    }


    @Test
    public void shouldUpdateStatusDetailFor2ndSnapshotStarted() {
        //given
        EventSourceNotification firstEventSourceNotification = createSnapshotEventSourceNotification("myStreamName", STARTED, "Loading snapshot");
        EventSourceNotification secondEventSourceNotification = createSnapshotEventSourceNotification("myStreamName2", STARTED, "Loading snapshot");

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(firstEventSourceNotification);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(secondEventSourceNotification);

        //then
        List<StatusDetail> statusDetails = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(0).getName(), is("myStreamName"));
        assertThat(statusDetails.get(0).getMessage(), is("Loading snapshot"));
        assertThat(statusDetails.get(1).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(1).getName(), is("myStreamName2"));
        assertThat(statusDetails.get(1).getMessage(), is("Loading snapshot"));
    }

    @Test
    public void shouldCalculateStartupTimes() {
        //given

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myStreamName", STARTED, "Loading snapshot"));
        testClock.proceed(2, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myStreamName2", STARTED, "Loading snapshot"));
        testClock.proceed(2, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myStreamName", FINISHED, "Done."));
        testClock.proceed(3, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myStreamName2", FINISHED, "Done."));

        //then
        List<StatusDetail> statusDetails = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(0).getName(), is("myStreamName"));
        assertThat(statusDetails.get(0).getMessage(), is("Done. Finished consumption after 4 seconds."));
        assertThat(statusDetails.get(1).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(1).getName(), is("myStreamName2"));
        assertThat(statusDetails.get(1).getMessage(), is("Done. Finished consumption after 5 seconds."));
    }

    private EventSourceNotification createSnapshotEventSourceNotification(final String streamName,
                                                                          final EventSourceNotification.Status status,
                                                                          final String message) {
        return EventSourceNotification.builder()
                .withStatus(status)
                .withEventSourceName("snapshot")
                .withStreamName(streamName)
                .withMessage(message)
                .build();
    }

}