package de.otto.synapse.aws.s3;

import com.google.common.collect.ImmutableList;
import de.otto.edison.aws.s3.S3Service;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;

import static de.otto.synapse.aws.s3.SnapshotServiceTestUtils.snapshotProperties;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class SnapshotReadServiceTest {

    private SnapshotReadService testee;
    private S3Service s3Service;
    private TempFileHelper tempFileHelper;

    @Before
    public void setUp() throws Exception {
        s3Service = mock(S3Service.class);
        tempFileHelper = mock(TempFileHelper.class);
        when(tempFileHelper.getTempDir()).thenCallRealMethod();

        testee = new SnapshotReadService(snapshotProperties(), s3Service, tempFileHelper);
    }


    @Test
    public void shouldDownloadLatestSnapshotFileFromBucket() throws Exception {
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
    public void shouldReturnOptionalEmptyWhenNoFileInBucket() throws Exception {
        //when
        when(s3Service.listAll(anyString())).thenReturn(emptyList());
        Optional<S3Object> s3Object = testee.fetchSnapshotMetadataFromS3("testBucket", "DOES_NOT_EXIST");

        //then
        assertThat(s3Object.isPresent(), is(false));
    }

    @Test
    public void shouldGetLatestSnapshotFileFromS3Bucket() throws Exception {
        //given
        when(tempFileHelper.getTempFile("compaction-teststream-snapshot-1.json.zip")).thenReturn(new File("/tmp/compaction-teststream-snapshot-1.json.zip").toPath());
        final S3Object s3Object = mock(S3Object.class);
        when(s3Object.key()).thenReturn("compaction-teststream-snapshot-1.json.zip");

        when(s3Service.listAll("test-teststream")).thenReturn(ImmutableList.of(s3Object));
        when(s3Service.download("test-teststream", "compaction-teststream-snapshot-1.json.zip", Paths.get("/tmp/compaction-teststream-snapshot-1.json.zip"))).thenReturn(true);

        //when
        Optional<File> file = testee.getLatestSnapshot("teststream");

        //then
        assertThat(file.get(), is(Paths.get("/tmp/compaction-teststream-snapshot-1.json.zip").toFile()));
    }

    @Test
    public void shouldReturnEmptyWhenThereIsNoSnapshotFileInS3Bucket() throws Exception {
        //given
        when(s3Service.listAll("test-test")).thenReturn(ImmutableList.of());

        //when
        Optional<File> file = testee.getLatestSnapshot("test");

        //then
        assertThat(file.isPresent(), is(false));
    }


    @Test
    public void shouldReturnEmptyWhenDownloadOfSnapshotFileFails() throws Exception {
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
    public void shouldUseLocalFileIfItsTheSameAsLatestFromBucket() {
        // given
        S3Object obj = S3Object.builder()
                .key("compaction-testStream-snapshot-1.json.zip")
                .size(55L)
                .build();
        when(s3Service.listAll("test-teststream")).thenReturn(ImmutableList.of(obj));
        File mockFile = mock(File.class);
        Path mockPath = mock(Path.class);
        when(mockPath.toFile()).thenReturn(mockFile);
        when(mockPath.toAbsolutePath()).thenReturn(Paths.get("/tmp/compaction-testStream-snapshot-1.json.zip"));
        when(tempFileHelper.getTempFile("compaction-testStream-snapshot-1.json.zip")).thenReturn(mockPath);
        when(tempFileHelper.existsAndHasSize(mockPath, 55L)).thenReturn(true);

        // when
        Optional<File> fileOptional = testee.retrieveLatestSnapshot("testStream");

        // then
        verify(s3Service, never()).download(any(), any(), any());
        assertThat(fileOptional, is(Optional.of(mockFile)));
    }
}
