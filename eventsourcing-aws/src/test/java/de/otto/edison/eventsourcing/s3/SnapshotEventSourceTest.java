package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SnapshotEventSourceTest {

    private static final String STREAM_NAME = "test-stream";

    @Mock
    private SnapshotReadService snapshotReadService;

    @Mock
    private SnapshotConsumerService snapshotConsumerService;

    @Mock
    private ApplicationEventPublisher applicationEventPublisher;

    private SnapshotEventSource snapshotEventSource;


    @Before
    public void init() {
        snapshotEventSource = new SnapshotEventSource(
                "snapshotEventSource",
                STREAM_NAME,
                snapshotReadService,
                snapshotConsumerService,
                applicationEventPublisher,
                new ObjectMapper());
        snapshotEventSource.register(EventConsumer.of(".*", String.class, (event)->{}));
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfDownloadFails() {
        // given
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.of(new File("someFilePath")));
        when(snapshotConsumerService.consumeSnapshot(any(),any(),any(),any())).thenThrow(new RuntimeException("boom - simulate exception while loading from S3"));

        // when
        snapshotEventSource.consumeAll();

        // then expect exception
    }

    @Test
    public void shouldThrowExceptionIfBucketNotExists() {
        // given
        S3Exception bucketNotFoundException = new S3Exception("boom - simulate exception while loading from S3");
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenThrow(bucketNotFoundException);

        // when
        try {
            snapshotEventSource.consumeAll();

            fail("should throw RuntimeException");
        } catch (RuntimeException ignored) {

        }

        // then
        EventSourceNotification expectedFailedEvent = EventSourceNotification.builder()
                .withEventSourceName("snapshotEventSource")
                .withStreamName(STREAM_NAME)
                .withStatus(EventSourceNotification.Status.FAILED)
                .withStreamPosition(SnapshotStreamPosition.of())
                .withMessage("Failed to load snapshot from S3: boom - simulate exception while loading from S3 (Service: null; Status Code: 0; Request ID: null)")
                .build();

        ArgumentCaptor<EventSourceNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(EventSourceNotification.class);
        verify(applicationEventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

        assertThat(notificationArgumentCaptor.getAllValues().get(1), is(expectedFailedEvent));
    }

    @Test
    public void shouldDeleteOlderSnapshotsInCaseOfAnException() {
        // given
        S3Exception bucketNotFoundException = new S3Exception("boom - simulate exception while loading from S3");
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenThrow(bucketNotFoundException);

        // when
        try {
            snapshotEventSource.consumeAll();
        } catch (Exception e) {
            // ignore exception
        }

        // then
        verify(snapshotReadService).deleteOlderSnapshots(STREAM_NAME);
    }

    @Test
    public void shouldPublishStartAndFinishEvents() {
        // given
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.empty());

        // when
        snapshotEventSource.consumeAll();

        // then
        EventSourceNotification expectedStartEvent = EventSourceNotification.builder()
                .withEventSourceName("snapshotEventSource")
                .withStreamName(STREAM_NAME)
                .withStatus(EventSourceNotification.Status.STARTED)
                .withMessage("Loading snapshot from S3.")
                .withStreamPosition(StreamPosition.of())
                .build();
        verify(applicationEventPublisher).publishEvent(expectedStartEvent);

        EventSourceNotification expectedFinishedEvent = EventSourceNotification.builder()
                .withEventSourceName("snapshotEventSource")
                .withStreamName(STREAM_NAME)
                .withStatus(EventSourceNotification.Status.FINISHED)
                .withMessage("Finished to load snapshot from S3.")
                .withStreamPosition(SnapshotStreamPosition.of())
                .build();
        verify(applicationEventPublisher).publishEvent(expectedFinishedEvent);
    }
}
