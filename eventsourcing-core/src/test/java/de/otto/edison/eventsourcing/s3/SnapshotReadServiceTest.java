package de.otto.edison.eventsourcing.s3;

import com.google.common.collect.ImmutableList;
import de.otto.edison.aws.s3.S3Service;
import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotReadServiceTest {

    private SnapshotReadService testee;
    private S3Service s3Service;

    @Before
    public void setUp() throws Exception {
        EventSourcingProperties eventSourcingProperties = SnapshotServiceTestUtils.createEventSourcingProperties();
        s3Service = mock(S3Service.class);
        testee = new SnapshotReadService(s3Service, eventSourcingProperties);
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
        Optional<S3Object> s3Object = testee.getLatestZip("testBucket", "test");

        //then
        assertThat(s3Object.get().key(), is("compaction-test-snapshot-2.json.zip"));
    }

    @Test
    public void shouldReturnOptionalEmptyWhenNoFileInBucket() throws Exception {
        //when
        when(s3Service.listAll(anyString())).thenReturn(emptyList());
        Optional<S3Object> s3Object = testee.getLatestZip("testBucket", "DOES_NOT_EXIST");

        //then
        assertThat(s3Object.isPresent(), is(false));
    }

    @Test
    public void shouldGetLatestSnapshotFileFromS3Bucket() throws Exception {
        //given
        final S3Object obj1 = mock(S3Object.class);
        when(obj1.key()).thenReturn("compaction-test-snapshot-1.json.zip");

        when(s3Service.listAll("test-test")).thenReturn(ImmutableList.of(obj1));
        when(s3Service.download("test-test", "compaction-test-snapshot-1.json.zip", Paths.get("/tmp/compaction-test-snapshot-1.json.zip"))).thenReturn(true);

        //when
        Optional<File> file = testee.getLatestSnapshotFromBucket("test");

        //then
        assertThat(file.get(), is(Paths.get("/tmp/compaction-test-snapshot-1.json.zip").toFile()));
    }

    @Test
    public void shouldReturnEmptyWhenThereIsNoSnapshotFileInS3Bucket() throws Exception {
        //given
        when(s3Service.listAll("test-test")).thenReturn(ImmutableList.of());

        //when
        Optional<File> file = testee.getLatestSnapshotFromBucket("test");

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
        Optional<File> file = testee.getLatestSnapshotFromBucket("test");

        //then
        assertThat(file.isPresent(), is(false));
    }
}
