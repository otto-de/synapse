package de.otto.synapse.messagestore.aws;

import de.otto.synapse.annotation.EnableEventSourcing;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.compaction.s3.CompactionService;
import de.otto.synapse.compaction.s3.SnapshotReadService;
import de.otto.synapse.compaction.s3.SnapshotWriteService;
import de.otto.synapse.helper.s3.S3Helper;
import de.otto.synapse.message.Message;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.testsupport.KinesisChannelSetupUtils;
import de.otto.synapse.testsupport.KinesisTestStreamSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(classes = S3SnapshotMessageStoreAcceptanceTest.class)
@TestPropertySource(properties = {
        "synapse.snapshot.bucket-name=de-otto-promo-compaction-test-snapshots",
        "synapse.compaction.enabled=true"}
)
@EnableEventSourcing
public class S3SnapshotMessageStoreAcceptanceTest {

    private static final String INTEGRATION_TEST_STREAM = "promo-compaction-test";
    private static final String INTEGRATION_TEST_BUCKET = "de-otto-promo-compaction-test-snapshots";

    @Autowired
    private KinesisAsyncClient kinesisClient;

    @Autowired
    private SnapshotWriteService snapshotWriteService;

    @Autowired
    private SnapshotReadService snapshotReadService;

    @Autowired
    private S3Client s3Client;

    private S3Helper s3Helper;

    @Autowired
    private CompactionService compactionService;

    @Autowired
    private StateRepository<String> stateRepository;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @Before
    public void setup() throws IOException {
        KinesisChannelSetupUtils.createChannelIfNotExists(kinesisClient, INTEGRATION_TEST_STREAM, 2);
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
    public void shouldWriteIntoMessageStoreFromStream() throws IOException {
        final ChannelPosition startSequenceNumbers = writeToStream(INTEGRATION_TEST_STREAM, "users_small1.txt").getFirstReadPosition();
        createInitialEmptySnapshotWithSequenceNumbers(startSequenceNumbers);

        //when
        compactionService.compact(INTEGRATION_TEST_STREAM);

        //then
        try (final S3SnapshotMessageStore snapshotMessageStore = new S3SnapshotMessageStore(INTEGRATION_TEST_STREAM, snapshotReadService, eventPublisher)) {
            final List<Message<String>> messages = new ArrayList<>();
            snapshotMessageStore.stream().forEach(messages::add);
            assertThat(messages, hasSize(10));
            assertThat(messages.stream().map(Message::getKey).collect(toList()), contains("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
            final ChannelPosition channelPosition = snapshotMessageStore.getLatestChannelPosition();
            assertThat(channelPosition, is(notNullValue()));
            assertThat(channelPosition.shards(), contains("shardId-000000000000", "shardId-000000000001"));
        }
    }

    private void createInitialEmptySnapshotWithSequenceNumbers(ChannelPosition startSequenceNumbers) throws IOException {
        snapshotWriteService.writeSnapshot(INTEGRATION_TEST_STREAM, startSequenceNumbers, stateRepository);
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
                .collect(toList());
    }

}
