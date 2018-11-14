package de.otto.synapse.compaction.s3;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

import static de.otto.synapse.compaction.s3.SnapshotServiceTestUtils.snapshotProperties;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotReadServiceTest {

    private SnapshotReadService testee;
    private S3Client s3Client;

    @Before
    public void setUp() {
        s3Client = mock(S3Client.class);

        testee = new SnapshotReadService(snapshotProperties(), s3Client);
    }


    @Test
    public void shouldDownloadLatestSnapshotFileFromBucket() {
        //given
        final S3Object obj1 = mock(S3Object.class);
        when(obj1.key()).thenReturn("compaction-test-snapshot-1.json.zip");
        when(obj1.lastModified()).thenReturn(Instant.MIN);
        final S3Object obj2 = mock(S3Object.class);
        when(obj2.key()).thenReturn("compaction-test-snapshot-2.json.zip");
        when(obj2.lastModified()).thenReturn(Instant.MAX);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(ListObjectsV2Response.builder().keyCount(2).contents(obj1, obj2).build());

        //when
        Optional<S3Object> s3Object = testee.fetchSnapshotMetadataFromS3("testBucket", "test");

        //then
        assertThat(s3Object.get().key(), is("compaction-test-snapshot-2.json.zip"));
    }

    @Test
    public void shouldReturnOptionalEmptyWhenNoFileInBucket() {
        //when
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(ListObjectsV2Response.builder().keyCount(0).contents(emptyList()).build());
        Optional<S3Object> s3Object = testee.fetchSnapshotMetadataFromS3("testBucket", "DOES_NOT_EXIST");

        //then
        assertThat(s3Object.isPresent(), is(false));
    }

    @Test
    public void shouldGetLatestSnapshotFileFromS3Bucket() {
        //given
        final S3Object s3Object = mock(S3Object.class);
        when(s3Object.key()).thenReturn("compaction-teststream-snapshot-1.json.zip");

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(ListObjectsV2Response.builder().keyCount(1).contents(s3Object).build());
        when(s3Client.getObject(any(GetObjectRequest.class), any(Path.class))).thenReturn(GetObjectResponse.builder().build());

        //when
        Optional<File> file = testee.getLatestSnapshot("teststream");

        //then
        assertThat(file.get().getPath(), Matchers.endsWith("/compaction-teststream-snapshot-1.json.zip"));
    }

    @Test
    public void shouldReturnEmptyWhenThereIsNoSnapshotFileInS3Bucket() {
        //given
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(ListObjectsV2Response.builder().keyCount(0).contents(emptyList()).build());

        //when
        Optional<File> file = testee.getLatestSnapshot("test");

        //then
        assertThat(file.isPresent(), is(false));
    }


    @Test
    public void shouldReturnEmptyWhenDownloadOfSnapshotFileFails() {
        //given
        final S3Object obj1 = mock(S3Object.class);
        when(obj1.key()).thenReturn("compaction-test-snapshot-1.json.zip");
        when(obj1.size()).thenReturn(123);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(ListObjectsV2Response.builder().keyCount(1).contents(obj1).build());
        when(s3Client.getObject(any(GetObjectRequest.class), any(Path.class))).thenThrow(NoSuchKeyException.class);

        //when
        Optional<File> file = testee.getLatestSnapshot("test");

        //then
        assertThat(file.isPresent(), is(false));
    }

    @Test
    public void shouldUseLocalFileIfItsTheSameAsLatestFromBucket() throws IOException {
        // given
        S3Object obj = S3Object.builder()
                .key("compaction-testStream-snapshot-1.json.zip")
                .size(0)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(ListObjectsV2Response.builder().keyCount(1).contents(obj).build());

        final Path tempFile = SnapshotFileHelper.getTempFile("/compaction-testStream-snapshot-1.json.zip");
        Files.deleteIfExists(tempFile);
        Files.createFile(tempFile);

        try {
            // when
            Optional<File> fileOptional = testee.retrieveLatestSnapshot("testStream");

            // then
//            verify(s3Helper, never()).download(any(), any(), any());
            assertThat(fileOptional.get().toPath(), is(tempFile));
        } finally {
            Files.delete(SnapshotFileHelper.getTempFile("/compaction-testStream-snapshot-1.json.zip"));
        }
    }
}
