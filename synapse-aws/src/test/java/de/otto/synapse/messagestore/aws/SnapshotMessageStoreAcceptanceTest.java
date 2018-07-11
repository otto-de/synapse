package de.otto.synapse.messagestore.aws;

import de.otto.edison.aws.s3.S3Service;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.testsupport.KinesisChannelSetupUtils;
import de.otto.synapse.compaction.aws.CompactionService;
import de.otto.synapse.compaction.aws.SnapshotReadService;
import de.otto.synapse.compaction.aws.SnapshotWriteService;
import de.otto.synapse.message.Message;
import de.otto.synapse.state.StateRepository;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.ByteBuffer;
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
@SpringBootTest(classes = SnapshotMessageStoreAcceptanceTest.class)
@TestPropertySource(properties = {
        "synapse.snapshot.bucket-name=de-otto-promo-compaction-test-snapshots",
        "synapse.compaction.enabled=true"}
)
public class SnapshotMessageStoreAcceptanceTest {

    private static final String INTEGRATION_TEST_STREAM = "promo-compaction-test";
    private static final String INTEGRATION_TEST_BUCKET = "de-otto-promo-compaction-test-snapshots";

    @Autowired
    private KinesisClient kinesisClient;

    @Autowired
    private S3Client s3Client;

    @Autowired
    private SnapshotWriteService snapshotWriteService;

    @Autowired
    private SnapshotReadService snapshotReadService;

    @Autowired
    private S3Service s3Service;

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
        s3Service.createBucket(INTEGRATION_TEST_BUCKET);
        s3Service.deleteAllObjectsInBucket(INTEGRATION_TEST_BUCKET);

        final ChannelPosition startSequenceNumbers = writeToStream(INTEGRATION_TEST_STREAM, "users_small1.txt").getFirstReadPosition();
        createInitialEmptySnapshotWithSequenceNumbers(startSequenceNumbers);
        compactionService.compact(INTEGRATION_TEST_STREAM);
    }

    @After
    public void tearDown() {
        s3Service.deleteAllObjectsInBucket(INTEGRATION_TEST_BUCKET);
    }

    @Test
    public void shouldReadSnapshot() throws Exception {
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
