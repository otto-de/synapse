package de.otto.synapse.compaction.aws;

import com.google.common.collect.ImmutableList;
import de.otto.edison.aws.s3.S3Service;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;

import static de.otto.synapse.compaction.aws.SnapshotServiceTestUtils.snapshotProperties;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class SnapshotReadServiceTest {

    private SnapshotReadService testee;
    private S3Service s3Service;

    @Before
    public void setUp() {
        s3Service = mock(S3Service.class);

        testee = new SnapshotReadService(snapshotProperties(), s3Service);
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
        when(s3Service.listAll("testBucket")).thenReturn(asList(obj1, obj2));

        //when
        Optional<S3Object> s3Object = testee.fetchSnapshotMetadataFromS3("testBucket", "test");

        //then
        assertThat(s3Object.get().key(), is("compaction-test-snapshot-2.json.zip"));
    }

    @Test
    public void shouldReturnOptionalEmptyWhenNoFileInBucket() {
        //when
        when(s3Service.listAll(anyString())).thenReturn(emptyList());
        Optional<S3Object> s3Object = testee.fetchSnapshotMetadataFromS3("testBucket", "DOES_NOT_EXIST");

        //then
        assertThat(s3Object.isPresent(), is(false));
    }

    @Test
    public void shouldGetLatestSnapshotFileFromS3Bucket() {
        //given
        final S3Object s3Object = mock(S3Object.class);
        when(s3Object.key()).thenReturn("compaction-teststream-snapshot-1.json.zip");

        when(s3Service.listAll("test-teststream")).thenReturn(ImmutableList.of(s3Object));
        when(s3Service.download(eq("test-teststream"), eq("compaction-teststream-snapshot-1.json.zip"), any(Path.class))).thenReturn(true);

        //when
        Optional<File> file = testee.getLatestSnapshot("teststream");

        //then
        assertThat(file.get().getPath(), Matchers.endsWith("/compaction-teststream-snapshot-1.json.zip"));
    }

    @Test
    public void shouldReturnEmptyWhenThereIsNoSnapshotFileInS3Bucket() {
        //given
        when(s3Service.listAll("test-test")).thenReturn(ImmutableList.of());

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
        when(obj1.size()).thenReturn(123L);
        when(s3Service.listAll("test-test")).thenReturn(ImmutableList.of(obj1));
        when(s3Service.download("test-test", "compaction-test-snapshot-1.json.zip", Paths.get("/tmp/compaction-test-snapshot-1.json.zip"))).thenReturn(false);

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
                .size(0L)
                .build();
        when(s3Service.listAll("test-teststream")).thenReturn(ImmutableList.of(obj));

        final Path tempFile = SnapshotFileHelper.getTempFile("/compaction-testStream-snapshot-1.json.zip");
        Files.deleteIfExists(tempFile);
        Files.createFile(tempFile);

        try {
            // when
            Optional<File> fileOptional = testee.retrieveLatestSnapshot("testStream");

            // then
            verify(s3Service, never()).download(any(), any(), any());
            assertThat(fileOptional.get().toPath(), is(tempFile));
        } finally {
            Files.delete(SnapshotFileHelper.getTempFile("/compaction-testStream-snapshot-1.json.zip"));
        }
    }
}
