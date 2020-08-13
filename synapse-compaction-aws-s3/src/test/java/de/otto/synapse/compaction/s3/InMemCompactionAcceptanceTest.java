package de.otto.synapse.compaction.s3;

import com.google.common.base.Charsets;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import de.otto.synapse.annotation.EnableEventSourcing;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.helper.s3.S3Helper;
import de.otto.synapse.message.Key;
import net.minidev.json.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;
import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(classes = {InMemCompactionAcceptanceTest.class, InMemoryMessageLogTestConfiguration.class})
@TestPropertySource(properties = {
        "spring.main.allow-bean-definition-overriding=true",
        "synapse.snapshot.bucket-name=de-otto-promo-compaction-test-snapshots",
        "synapse.compaction.enabled=true"}
)
@EnableEventSourcing
@DirtiesContext
@EnableMessageSenderEndpoint(name = "compactionTestSender", channelName = "promo-compaction-test", selector = MessageLog.class)
public class InMemCompactionAcceptanceTest {

    private static final String INTEGRATION_TEST_STREAM = "promo-compaction-test";
    private static final String INTEGRATION_TEST_BUCKET = "de-otto-promo-compaction-test-snapshots";

    @Autowired
    private MessageSenderEndpoint compactionTestSender;

    @Autowired
    private S3Client s3Client;

    @Autowired
    private CompactionService compactionService;

    private S3Helper s3Helper;

    @Before
    public void setup() throws IOException {
        deleteSnapshotFilesFromTemp();
        s3Helper = new S3Helper(s3Client);
        s3Helper.createBucket(INTEGRATION_TEST_BUCKET);
        s3Helper.deleteAllObjectsInBucket(INTEGRATION_TEST_BUCKET);
    }

    @After
    public void tearDown() {
        s3Helper.deleteAllObjectsInBucket(INTEGRATION_TEST_BUCKET);
    }

    @Test
    public void shouldCompactData() throws Exception {
        //given
        sendTestMessages(Range.closed(1, 100), "first");

        String filenameBefore = compactionService.compact(INTEGRATION_TEST_STREAM);

        LinkedHashMap<String, JSONArray> json1 = fetchAndParseSnapshotFileFromS3(filenameBefore);
        assertSnapshotFileStructureAndSize(json1, 100);

        //when write additional data with partially existing ids
        sendTestMessages(Range.closed(50, 150), "second");

        //Write an emptyMessageStore object for of 100000 - should be removed during compaction
        compactionTestSender.send(message("100000", null));


        String fileName = compactionService.compact(INTEGRATION_TEST_STREAM);

        //then
        LinkedHashMap<String, JSONArray> json2 = fetchAndParseSnapshotFileFromS3(fileName);

        assertSnapshotFileStructureAndSize(json2, 150);

        assertMessageForKey(json2, "1", "first-1");
        assertMessageForKey(json2, "49", "first-49");
        assertMessageForKey(json2, "50", "second-50");
        assertMessageForKey(json2, "150", "second-150");

        assertMessageDoesNotExist(json2, "151");
        assertMessageDoesNotExist(json2, "100000");

    }

    @Test
    public void shouldCompactDataWithCompoundKeys() throws Exception {
        //given
        sendTestMessagesWithCompoundKey(Range.closed(1000, 1100), "first");

        String filenameBefore = compactionService.compact(INTEGRATION_TEST_STREAM);

        LinkedHashMap<String, JSONArray> json1 = fetchAndParseSnapshotFileFromS3(filenameBefore);
        assertSnapshotFileStructureAndSize(json1, 200);

        //when write additional data with partially existing ids
        sendTestMessagesWithCompoundKey(Range.closed(1050, 1150), "second");

        //Write an emptyMessageStore object for of 100000 - should be removed during compaction
        compactionTestSender.send(message("110000", null));


        String fileName = compactionService.compact(INTEGRATION_TEST_STREAM);

        //then
        LinkedHashMap<String, JSONArray> json2 = fetchAndParseSnapshotFileFromS3(fileName);

        assertSnapshotFileStructureAndSize(json2, 300);

        assertMessageForKey(json2, "PRICE#1000", "first-1000");
        assertMessageForKey(json2, "AVAILABILITY#1000", "first-1000");
        assertMessageForKey(json2, "PRICE#1049", "first-1049");
        assertMessageForKey(json2, "AVAILABILITY#1049", "first-1049");
        assertMessageForKey(json2, "PRICE#1050", "second-1050");
        assertMessageForKey(json2, "AVAILABILITY#1050", "second-1050");
        assertMessageForKey(json2, "PRICE#1150", "second-1150");
        assertMessageForKey(json2, "AVAILABILITY#1150", "second-1150");

        assertMessageDoesNotExist(json2, "PRICE#1151");
        assertMessageDoesNotExist(json2, "AVAILABILITY#1151");
        assertMessageDoesNotExist(json2, "110000");
    }

    @SuppressWarnings("unchecked")
    private LinkedHashMap<String, JSONArray> fetchAndParseSnapshotFileFromS3(String snapshotFileName) {
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

        assertThat(json, hasJsonPath("$.data", hasSize(greaterThanOrEqualTo(expectedNumberOfRecords))));
    }

    private void assertMessageForKey(LinkedHashMap<String, JSONArray> json, final String key, String expectedPayload) {
        JSONArray jsonArray = JsonPath.read(json, "$.data[?(@." + key + ")]." + key);
        assertThat(jsonArray.size(), is(greaterThan(0)));
        assertThat(jsonArray.get(0).toString(), is(expectedPayload));
    }

    private void assertMessageDoesNotExist(LinkedHashMap<String, JSONArray> json, final String key) {
        JSONArray jsonArray = JsonPath.read(json, "$.data[?(@." + key + ")]." + key);
        assertThat(jsonArray.isEmpty(), is(true));
    }

    private void sendTestMessages(final Range<Integer> messageKeyRange, final String payloadPrefix) throws InterruptedException {
        ContiguousSet.create(messageKeyRange, DiscreteDomain.integers())
                .forEach(key -> compactionTestSender.send(message(valueOf(key), payloadPrefix + "-" + key)).join());
        sleep(20);
    }

    private void sendTestMessagesWithCompoundKey(final Range<Integer> messageKeyRange, final String payloadPrefix) throws InterruptedException {
        ContiguousSet.create(messageKeyRange, DiscreteDomain.integers())
                .forEach(key -> compactionTestSender.send(message(Key.of(valueOf(key), "PRICE#" + key), payloadPrefix + "-" + key)).join());
        ContiguousSet.create(messageKeyRange, DiscreteDomain.integers())
                .forEach(key -> compactionTestSender.send(message(Key.of(valueOf(key), "AVAILABILITY#" + key), payloadPrefix + "-" + key)).join());
        sleep(20);
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
        try (Stream<Path> filesStream = Files.list(Paths.get(System.getProperty("java.io.tmpdir")))) {
            return filesStream
                    .filter(p -> p.toFile().getName().startsWith("compaction-promo-compaction-test-snapshot-"))
                    .collect(Collectors.toList());
        }
    }

}
