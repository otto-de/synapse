package de.otto.synapse.compaction.s3;

import de.otto.synapse.annotation.EnableEventSourcing;
import de.otto.synapse.configuration.EventSourcingAutoConfiguration;
import de.otto.synapse.helper.s3.S3Helper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.util.Optional;

import static java.nio.file.Files.createTempFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(classes = SnapshotReadServiceIntegrationTest.class)
@EnableEventSourcing
public class SnapshotReadServiceIntegrationTest {

    private static final String S3_UTILS_TEST_BUCKET = "de-otto-promo-compaction-test-snapshots";

    @Autowired
    private S3Client s3Client;

    @Autowired
    private SnapshotReadService snapshotService;

    private S3Helper s3Helper;

    @Before
    public void setUp() {
        s3Helper = new S3Helper(s3Client);
        s3Helper.createBucket(S3_UTILS_TEST_BUCKET);
        s3Helper.deleteAllObjectsInBucket(S3_UTILS_TEST_BUCKET);
    }

    @After
    public void tearDown() {
        s3Helper.deleteAllObjectsInBucket(S3_UTILS_TEST_BUCKET);
    }

    @Test
    public void shouldDownloadLatestSnapshotFileFromBucket() throws Exception {
        //given
        s3Helper.upload(S3_UTILS_TEST_BUCKET, createTempFile("compaction-test-snapshot-", ".json.zip").toFile());

        waitSoThatNextSnapshotHasDifferentModificationDate();

        File latest = createTempFile("compaction-test-snapshot-", ".json.zip").toFile();
        s3Helper.upload(S3_UTILS_TEST_BUCKET, latest);

        //when
        Optional<S3Object> s3Object = snapshotService.fetchSnapshotMetadataFromS3(S3_UTILS_TEST_BUCKET, "test");

        //then
        assertThat(s3Object.get().key(), is(latest.getName()));
    }

    @Test
    public void shouldReturnOptionalEmptyWhenNoFileInBucket() {
        //when
        Optional<S3Object> s3Object = snapshotService.fetchSnapshotMetadataFromS3(S3_UTILS_TEST_BUCKET, "DOES_NOT_EXIST");

        //then
        assertThat(s3Object.isPresent(), is(false));
    }

    private void waitSoThatNextSnapshotHasDifferentModificationDate() throws InterruptedException {
        Thread.sleep(1000);
    }
}
