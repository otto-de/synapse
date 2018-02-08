package de.otto.edison.eventsourcing.statusdetail;

import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.kinesis.KinesisStream;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;
import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.testsupport.util.TestClock;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.List;

import static de.otto.edison.eventsourcing.consumer.EventSourceNotification.Status.FINISHED;
import static de.otto.edison.eventsourcing.consumer.EventSourceNotification.Status.STARTED;
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
        EventSourceNotification eventSourceNotification = createSnapshotEventSourceNotification("myStreamName", STARTED);

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
        EventSourceNotification firstEventSourceNotification = createSnapshotEventSourceNotification("myStreamName", STARTED);
        EventSourceNotification secondEventSourceNotification = createSnapshotEventSourceNotification("myStreamName2", STARTED);

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
    public void shouldCalculateStartupTimesCorrectlyForSnapshotEventSource() {
        //given

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myStreamName", STARTED));
        testClock.proceed(2, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myStreamName2", STARTED));
        testClock.proceed(2, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myStreamName", FINISHED));
        testClock.proceed(3, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createSnapshotEventSourceNotification("myStreamName2", FINISHED));

        //then
        List<StatusDetail> statusDetails = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(0).getName(), is("myStreamName"));
        assertThat(statusDetails.get(0).getMessage(), is("Startup time was 4 seconds."));
        assertThat(statusDetails.get(1).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(1).getName(), is("myStreamName2"));
        assertThat(statusDetails.get(1).getMessage(), is("Startup time was 5 seconds."));
    }
    @Test
    public void shouldCalculateStartupTimesCorrectlyForKinesisEventSource() {
        //given

        //when
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createKinesisEventSourceNotification("myStreamName", STARTED));
        testClock.proceed(2, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createKinesisEventSourceNotification("myStreamName2", STARTED));
        testClock.proceed(2, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createKinesisEventSourceNotification("myStreamName", FINISHED));
        testClock.proceed(3, ChronoUnit.SECONDS);
        eventSourcingStatusDetailIndicator.onEventSourceNotification(createKinesisEventSourceNotification("myStreamName2", FINISHED));

        //then
        List<StatusDetail> statusDetails = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(0).getName(), is("myStreamName"));
        assertThat(statusDetails.get(0).getMessage(), is("Consumer finished in 4 seconds."));
        assertThat(statusDetails.get(1).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(1).getName(), is("myStreamName2"));
        assertThat(statusDetails.get(1).getMessage(), is("Consumer finished in 5 seconds."));
    }

    @Test
    public void shouldUpdateStatusDetailForKinesisEventSource() {
        //given
        EventSourceNotification firstEventSourceNotification = createSnapshotEventSourceNotification("myStreamName", STARTED);
        EventSourceNotification secondEventSourceNotification = createKinesisEventSourceNotification("myStreamName2", STARTED);

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
        assertThat(statusDetails.get(1).getMessage(), is("Consuming from kinesis."));
    }

    private EventSourceNotification createSnapshotEventSourceNotification(String streamName, EventSourceNotification.Status status) {
        return EventSourceNotification.builder()
                .withStatus(status)
                .withEventSource(new SnapshotEventSource("esName", streamName, null, null, null, null))
                .build();
    }

    private EventSourceNotification createKinesisEventSourceNotification(String streamName, EventSourceNotification.Status status) {
        return EventSourceNotification.builder()
                .withStatus(status)
                .withEventSource(new KinesisEventSource("esName", new KinesisStream(null, streamName), null, null))
                .build();
    }
}