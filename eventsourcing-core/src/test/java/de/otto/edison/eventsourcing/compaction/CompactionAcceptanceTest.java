package de.otto.edison.eventsourcing.compaction;

import com.google.common.base.Charsets;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import de.otto.edison.aws.s3.S3Service;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.kinesis.KinesisStreamSetupUtils;
import de.otto.edison.eventsourcing.s3.SnapshotReadServiceIntegrationTest;
import de.otto.edison.eventsourcing.s3.SnapshotWriteService;
import de.otto.edison.eventsourcing.state.StateRepository;
import de.otto.edison.eventsourcing.testsupport.TestStreamSource;
import net.minidev.json.JSONArray;
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
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.core.sync.ResponseInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;
import java.util.zip.ZipInputStream;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.edison.eventsourcing"})
@SpringBootTest(classes = SnapshotReadServiceIntegrationTest.class)
public class CompactionAcceptanceTest {

    private static final String INTEGRATION_TEST_STREAM = "promo-compaction-test";
    private static final String INTEGRATION_TEST_BUCKET = "de-otto-promo-compaction-test-snapshots";

    @Autowired
    private KinesisClient kinesisClient;

    @Autowired
    private S3Client s3Client;

    @Autowired
    private SnapshotWriteService snapshotWriteService;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private CompactionService compactionService;

    @Autowired
    private StateRepository<String> stateRepository;

    @Before
    public void setup() {
        KinesisStreamSetupUtils.createStreamIfNotExists(kinesisClient, INTEGRATION_TEST_STREAM, 2);
        s3Service.createBucket(INTEGRATION_TEST_BUCKET);
        s3Service.deleteAllObjectsInBucket(INTEGRATION_TEST_BUCKET);
    }

    @After
    public void tearDown() {
        s3Service.deleteAllObjectsInBucket(INTEGRATION_TEST_BUCKET);
    }

    @Test
    public void shouldCompactData() throws Exception {
        //given
        int firstWriteElementCount = 10;
        int secondWriteElementCount = 5;
        int fakeElementCount = 2;

        StreamPosition startSequenceNumbers = writeToStream(INTEGRATION_TEST_STREAM, "users_small1.txt").getFirstReadPosition();
        createInitialEmptySnapshotWithSequenceNumbers(startSequenceNumbers);

        String filenameBefore = compactionService.compact(INTEGRATION_TEST_STREAM);

        LinkedHashMap<String, JSONArray> json1 = fetchAndParseSnapshotFileFromS3(filenameBefore);
        assertSnapshotFileStructureAndSize(json1, firstWriteElementCount);

        //when write additional data with partially existing ids
        writeToStream(INTEGRATION_TEST_STREAM, "integrationtest-stream.txt");

        String fileName = compactionService.compact(INTEGRATION_TEST_STREAM);

        //then
        LinkedHashMap<String, JSONArray> json2 = fetchAndParseSnapshotFileFromS3(fileName);
        assertSnapshotFileStructureAndSize(json2, firstWriteElementCount + secondWriteElementCount + fakeElementCount);
        assertUsernameForUserId(json2, "1", "Frank");
        assertUsernameForUserId(json2, "2", "PGL08LJI");
        assertUsernameForUserId(json2, "20000", "PGL08LJI");
        assertUsernameForUserId(json2, "3", "Horst");
    }

    private void createInitialEmptySnapshotWithSequenceNumbers(StreamPosition startSequenceNumbers) throws IOException {
        snapshotWriteService.takeSnapshot(INTEGRATION_TEST_STREAM, startSequenceNumbers, stateRepository);
    }

    @SuppressWarnings("unchecked")
    private LinkedHashMap<String, JSONArray> fetchAndParseSnapshotFileFromS3(String snapshotFileName) throws ExecutionException, InterruptedException {
        GetObjectRequest request = GetObjectRequest.builder().bucket(INTEGRATION_TEST_BUCKET).key(snapshotFileName).build();

        ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(request);
        try (
                BufferedInputStream bufferedInputStream = new BufferedInputStream(responseInputStream);
                ZipInputStream zipInputStream = new ZipInputStream(bufferedInputStream)
        ) {
            zipInputStream.getNextEntry();
            return (LinkedHashMap<String, JSONArray>) Configuration.defaultConfiguration().jsonProvider().parse(zipInputStream, Charsets.UTF_8.name());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertSnapshotFileStructureAndSize(LinkedHashMap<String, JSONArray> json, int expectedNumberOfRecords) {
        assertThat(json, hasJsonPath("$.startSequenceNumbers[0].shard", not(empty())));
        assertThat(json, hasJsonPath("$.startSequenceNumbers[0].sequenceNumber", not(empty())));

        assertThat(json, hasJsonPath("$.data", hasSize(expectedNumberOfRecords)));
    }

    private void assertUsernameForUserId(LinkedHashMap<String, JSONArray> json, final String userId, String expectedUserName) {
        JSONArray jsonArray = JsonPath.read(json, "$.data[?(@." + userId + ")]." + userId);
        assertThat(jsonArray.get(0).toString(), hasJsonPath("$.username", is(expectedUserName)));
    }

    private TestStreamSource writeToStream(String streamName, String fileName) {
        TestStreamSource streamSource = new TestStreamSource(kinesisClient, streamName, fileName);
        streamSource.writeToStream();
        return streamSource;
    }

}
