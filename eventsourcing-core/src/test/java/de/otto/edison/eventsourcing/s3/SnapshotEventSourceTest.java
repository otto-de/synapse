package de.otto.edison.eventsourcing.s3;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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

}