package de.otto.synapse.compaction.aws;

import com.google.common.base.Charsets;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import de.otto.edison.aws.s3.S3Service;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.testsupport.KinesisChannelSetupUtils;
import de.otto.synapse.testsupport.KinesisTestStreamSource;
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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(classes = CompactionAcceptanceTest.class)
@TestPropertySource(properties = {
        "synapse.snapshot.bucket-name=de-otto-promo-compaction-test-snapshots",
        "synapse.compaction.enabled=true"}
)
public class CompactionAcceptanceTest {

    private static final String INTEGRATION_TEST_STREAM = "promo-compaction-test";
    private static final String INTEGRATION_TEST_BUCKET = "de-otto-promo-compaction-test-snapshots";
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[]{});

    @Autowired
    private KinesisClient kinesisClient;

    @Autowired
    private S3Client s3Client;

    @Autowired
    private SnapshotWriteService snapshotWriteService;


    @Autowired
    private CompactionService compactionService;

    @Autowired
    private StateRepository<String> stateRepository;

    private S3Service s3Service;

    @Before
    public void setup() throws IOException {
        KinesisChannelSetupUtils.createChannelIfNotExists(kinesisClient, INTEGRATION_TEST_STREAM, 2);
        deleteSnapshotFilesFromTemp();
        s3Service = new S3Service(s3Client);
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
        int deletedElementCount = 1;

        ChannelPosition startSequenceNumbers = writeToStream(INTEGRATION_TEST_STREAM, "users_small1.txt").getFirstReadPosition();
        createInitialEmptySnapshotWithSequenceNumbers(startSequenceNumbers);

        String filenameBefore = compactionService.compact(INTEGRATION_TEST_STREAM);

        LinkedHashMap<String, JSONArray> json1 = fetchAndParseSnapshotFileFromS3(filenameBefore);
        assertSnapshotFileStructureAndSize(json1, firstWriteElementCount);

        //when write additional data with partially existing ids
        writeToStream(INTEGRATION_TEST_STREAM, "integrationtest-stream.txt");

        //Write an emptyMessageStore object for key 100000 - should be removed during compaction
        kinesisClient.putRecord(PutRecordRequest.builder().streamName(INTEGRATION_TEST_STREAM).partitionKey("100000").data(EMPTY_BYTE_BUFFER).build());


        String fileName = compactionService.compact(INTEGRATION_TEST_STREAM);

        //then
        LinkedHashMap<String, JSONArray> json2 = fetchAndParseSnapshotFileFromS3(fileName);
        assertSnapshotFileStructureAndSize(json2, firstWriteElementCount + secondWriteElementCount + fakeElementCount - deletedElementCount);
        assertUsernameForUserId(json2, "1", "Frank");
        assertUsernameForUserId(json2, "2", "PGL08LJI");
        assertUsernameForUserId(json2, "20000", "PGL08LJI");
        assertUsernameForUserId(json2, "3", "Horst");

        assertUserIdDoesNotExist(json2, "100000");

    }

    private void createInitialEmptySnapshotWithSequenceNumbers(ChannelPosition startSequenceNumbers) throws IOException {
        snapshotWriteService.writeSnapshot(INTEGRATION_TEST_STREAM, startSequenceNumbers, stateRepository);
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

    private void assertUserIdDoesNotExist(LinkedHashMap<String, JSONArray> json, final String userId) {
        JSONArray jsonArray = JsonPath.read(json, "$.data[?(@." + userId + ")]." + userId);
        assertThat(jsonArray.isEmpty(), is(true));
    }

    private KinesisTestStreamSource writeToStream(String channelName, String fileName) {
        KinesisTestStreamSource streamSource = new KinesisTestStreamSource(kinesisClient, channelName, fileName);
        streamSource.writeToStream();
        return streamSource;
    }

    private void deleteSnapshotFilesFromTemp() throws IOException {
        getSnapshotFilePaths()
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private List<Path> getSnapshotFilePaths() throws IOException {
        return Files.list(Paths.get(System.getProperty("java.io.tmpdir")))
                .filter(p -> p.toFile().getName().startsWith("compaction-promo-compaction-test-snapshot-"))
                .collect(Collectors.toList());
    }

}
