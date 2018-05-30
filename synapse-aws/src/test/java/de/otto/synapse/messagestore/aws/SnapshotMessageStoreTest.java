package de.otto.synapse.messagestore.aws;

import de.otto.synapse.compaction.aws.SnapshotReadService;
import de.otto.synapse.info.SnapshotReaderNotification;
import de.otto.synapse.info.SnapshotReaderStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.ClassPathResource;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static de.otto.synapse.info.SnapshotReaderNotification.builder;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SnapshotMessageStoreTest {

    private static final String STREAM_NAME = "test-stream";

    @Mock
    private SnapshotReadService snapshotReadService;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfDownloadFails() {
        // given
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.of(new File("someFileWithPattern-snapshot-2018-01-01T00-00Z-1234567890123456789.json.zip")));

        // when
        new SnapshotMessageStore(STREAM_NAME, snapshotReadService, eventPublisher);

        // then expect exception
    }

    @Test
    public void shouldThrowExceptionIfBucketNotExists() {
        // given
        S3Exception bucketNotFoundException = new S3Exception("boom - simulate exception while loading from S3");
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenThrow(bucketNotFoundException);

        // when
        try {
            new SnapshotMessageStore(STREAM_NAME, snapshotReadService, eventPublisher);

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
        verify(eventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

        assertThat(notificationArgumentCaptor.getAllValues().get(1), is(expectedFailedEvent));
    }

    @Test
    public void shouldPublishStartingAndFinishEvents() throws IOException {
        // given
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.empty());

        // when
        final SnapshotMessageStore messageStore = new SnapshotMessageStore(STREAM_NAME, snapshotReadService, eventPublisher);
        messageStore.close();

        // then
        SnapshotReaderNotification expectedStartEvent = builder()
                .withChannelName(STREAM_NAME)
                .withStatus(SnapshotReaderStatus.STARTING)
                .withMessage("Retrieve snapshot file from S3.")
                .build();
        verify(eventPublisher).publishEvent(expectedStartEvent);

        SnapshotReaderNotification expectedFinishedEvent = builder()
                .withChannelName(STREAM_NAME)
                .withStatus(SnapshotReaderStatus.FINISHED)
                .withMessage("Finished to load snapshot from S3.")
                .build();
        verify(eventPublisher).publishEvent(expectedFinishedEvent);
        verifyNoMoreInteractions(eventPublisher);
    }

    //@Test
    public void measureRuntimeFor5000Messages() throws IOException {
        final File bigFile = new ClassPathResource("compaction-integrationtest-snapshot-2017-09-29T09-02Z-3053797267191232636.json.zip").getFile();
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.of(bigFile));

        long ts = System.currentTimeMillis();
        final SnapshotMessageStore messageStore = new SnapshotMessageStore(STREAM_NAME, snapshotReadService, eventPublisher);
        try {
            messageStore.stream().forEach((m) -> m.getKey());
        } finally {
            messageStore.close();
        }
        System.out.println("Consuming messages took " + (System.currentTimeMillis()-ts) + "ms.");
    }

}
