package de.otto.edison.eventsourcing.s3;

import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SnapshotEventSourceTest {

    @Mock
    private SnapshotReadService snapshotReadService;

    @Mock
    private SnapshotConsumerService snapshotConsumerService;

    private SnapshotEventSource snapshotEventSource;
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        snapshotEventSource = new SnapshotEventSource("streamName", snapshotReadService, snapshotConsumerService, String.class);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfDownloadFails() throws Exception {
        // given
        when(snapshotReadService.downloadLatestSnapshot(any())).thenReturn(Optional.of(new File("someFilePath")));
        when(snapshotConsumerService.consumeSnapshot(any(),any(),any(),any(),any())).thenThrow(new IOException("boom - simulate exception while loading from S3"));

        // when
        snapshotEventSource.consumeAll((event) -> {});

        // then expect exception
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfBucketNotExists() throws Exception {
        // given
        S3Exception bucketNotFoundException = new S3Exception("boom - simulate exception while loading from S3");
        bucketNotFoundException.setErrorCode("NoSuchBucket");
        when(snapshotReadService.downloadLatestSnapshot(any())).thenThrow(bucketNotFoundException);

        // when
        snapshotEventSource.consumeAll((event) -> {});

        // then expect exception
    }

    @Test
    public void shouldDeleteOlderSnapshotsInCaseOfAnException() throws Exception {
        // given
        S3Exception bucketNotFoundException = new S3Exception("boom - simulate exception while loading from S3");
        bucketNotFoundException.setErrorCode("NoSuchBucket");
        when(snapshotReadService.downloadLatestSnapshot(any())).thenThrow(bucketNotFoundException);

        // when
        try {
            snapshotEventSource.consumeAll((event) -> {});
        } catch (Exception e) {
            // ignore exception
        }

        // then
        verify(snapshotReadService).deleteOlderSnapshots("streamName");
    }

    @Test
    public void shouldPublishStartAndFinishEvents() throws Exception {
        // given
        when(snapshotReadService.downloadLatestSnapshot(any())).thenReturn(Optional.empty());

        ApplicationEventPublisher applicationEventPublisher = Mockito.mock(ApplicationEventPublisher.class);
        snapshotEventSource.setEventPublisher(applicationEventPublisher);

        // when
        snapshotEventSource.consumeAll((event) -> {});

        // then
        EventSourceNotification expectedStartEvent = EventSourceNotification.builder()
                .withEventSource(snapshotEventSource)
                .withStatus(EventSourceNotification.Status.STARTED)
                .withStreamPosition(StreamPosition.of())
                .build();
        verify(applicationEventPublisher).publishEvent(expectedStartEvent);

        EventSourceNotification expectedFinishedEvent = EventSourceNotification.builder()
                .withEventSource(snapshotEventSource)
                .withStatus(EventSourceNotification.Status.FINISHED)
                .withStreamPosition(SnapshotStreamPosition.of())
                .build();
        verify(applicationEventPublisher).publishEvent(expectedFinishedEvent);
    }
}