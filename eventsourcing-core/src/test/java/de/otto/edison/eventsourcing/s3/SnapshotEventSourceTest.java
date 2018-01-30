package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.util.Optional;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    public void shouldThrowExceptionIfDownloadFails() throws Exception {
        // given
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.of(new File("someFilePath")));
        when(snapshotConsumerService.consumeSnapshot(any(),any(),any(),any())).thenThrow(new RuntimeException("boom - simulate exception while loading from S3"));

        // when
        snapshotEventSource.consumeAll();

        // then expect exception
    }

    @Test
    public void shouldThrowExceptionIfBucketNotExists() throws Exception {
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
        EventSourceNotification expectedStartEvent = EventSourceNotification.builder()
                .withEventSource(snapshotEventSource)
                .withStatus(EventSourceNotification.Status.FAILED)
                .withStreamPosition(SnapshotStreamPosition.of())
                .build();
        verify(applicationEventPublisher).publishEvent(expectedStartEvent);
    }

    @Test
    public void shouldDeleteOlderSnapshotsInCaseOfAnException() throws Exception {
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
    public void shouldPublishStartAndFinishEvents() throws Exception {
        // given
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.empty());

        // when
        snapshotEventSource.consumeAll();

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
