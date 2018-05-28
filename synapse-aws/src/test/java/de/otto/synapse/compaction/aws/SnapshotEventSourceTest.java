package de.otto.synapse.compaction.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.info.SnapshotReaderNotification;
import de.otto.synapse.info.SnapshotReaderStatus;
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

import static de.otto.synapse.info.SnapshotReaderNotification.builder;
import static org.hamcrest.CoreMatchers.is;
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
    private ApplicationEventPublisher applicationEventPublisher;

    private SnapshotEventSource snapshotEventSource;


    @Before
    public void init() {
        snapshotEventSource = new SnapshotEventSource(
                "snapshotEventSource",
                STREAM_NAME,
                snapshotReadService,
                applicationEventPublisher,
                new ObjectMapper());
        snapshotEventSource.register(MessageConsumer.of(".*", String.class, (event)->{}));
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfDownloadFails() {
        // given
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.of(new File("someFileWithPattern-snapshot-2018-01-01T00-00Z-1234567890123456789.json.zip")));

        // when
        snapshotEventSource.consume();

        // then expect exception
    }

    @Test
    public void shouldThrowExceptionIfBucketNotExists() {
        // given
        S3Exception bucketNotFoundException = new S3Exception("boom - simulate exception while loading from S3");
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenThrow(bucketNotFoundException);

        // when
        try {
            snapshotEventSource.consume();

            fail("should throw RuntimeException");
        } catch (RuntimeException ignored) {

        }

        // then
        SnapshotReaderNotification expectedFailedEvent = builder()
                .withChannelName(STREAM_NAME)
                .withStatus(SnapshotReaderStatus.FAILED)
                .withMessage("Failed to load snapshot from S3: boom - simulate exception while loading from S3 (Service: null; Status Code: 0; Request ID: null)")
                .build();

        ArgumentCaptor<SnapshotReaderNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(SnapshotReaderNotification.class);
        verify(applicationEventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

        assertThat(notificationArgumentCaptor.getAllValues().get(1), is(expectedFailedEvent));
    }

    @Test
    public void shouldPublishStartingAndFinishEvents() {
        // given
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.empty());

        // when
        snapshotEventSource.consume();

        // then
        SnapshotReaderNotification expectedStartEvent = builder()
                .withChannelName(STREAM_NAME)
                .withStatus(SnapshotReaderStatus.STARTING)
                .withMessage("Retrieve snapshot file from S3.")
                .build();
        verify(applicationEventPublisher).publishEvent(expectedStartEvent);

        SnapshotReaderNotification expectedFinishedEvent = builder()
                .withChannelName(STREAM_NAME)
                .withStatus(SnapshotReaderStatus.FINISHED)
                .withMessage("Finished to load snapshot from S3.")
                .build();
        verify(applicationEventPublisher).publishEvent(expectedFinishedEvent);
        verifyNoMoreInteractions(applicationEventPublisher);
    }
}
