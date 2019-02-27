package de.otto.synapse.messagestore.aws;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import de.otto.synapse.annotation.EnableEventSourcing;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.compaction.s3.CompactionService;
import de.otto.synapse.compaction.s3.SnapshotReadService;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.helper.s3.S3Helper;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.messagestore.MessageStoreEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;
import static java.lang.Thread.sleep;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(classes = {
        S3SnapshotMessageStoreAcceptanceTest.class,
        InMemoryMessageLogTestConfiguration.class
})
@TestPropertySource(properties = {
        "spring.main.allow-bean-definition-overriding=true",
        "synapse.snapshot.bucket-name=de-otto-promo-compaction-test-snapshots",
        "synapse.compaction.enabled=true"}
)
@EnableEventSourcing
@EnableMessageSenderEndpoint(name = "compactionTestSender", channelName = "promo-compaction-test", selector = MessageLog.class)
@DirtiesContext
public class S3SnapshotMessageStoreAcceptanceTest {

    private static final String INTEGRATION_TEST_STREAM = "promo-compaction-test";
    private static final String INTEGRATION_TEST_BUCKET = "de-otto-promo-compaction-test-snapshots";

    @Autowired
    private MessageSenderEndpoint compactionTestSender;

    @Autowired
    private SnapshotReadService snapshotReadService;

    @Autowired
    private S3Client s3Client;

    private S3Helper s3Helper;

    @Autowired
    private CompactionService compactionService;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

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
    public void shouldWriteIntoMessageStoreFromStream() throws InterruptedException {
        sendTestMessages(Range.closed(1, 10), "first");

        //when
        compactionService.compact(INTEGRATION_TEST_STREAM);

        //then
        try (final S3SnapshotMessageStore snapshotMessageStore = new S3SnapshotMessageStore("Snapshot", INTEGRATION_TEST_STREAM, snapshotReadService, eventPublisher)) {
            final List<Message<String>> messages = new ArrayList<>();
            snapshotMessageStore.streamAll().map(MessageStoreEntry::getTextMessage).forEach(messages::add);
            assertThat(messages, hasSize(10));
            assertThat(messages.stream().map(Message::getKey).map(Key::partitionKey).collect(toList()), contains("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
            final ChannelPosition channelPosition = snapshotMessageStore.getLatestChannelPosition();
            assertThat(channelPosition, is(channelPosition(fromPosition("promo-compaction-test", "9"))));
        }
    }

    private void sendTestMessages(final Range<Integer> messageKeyRange, final String payloadPrefix) throws InterruptedException {
        ContiguousSet
                .create(messageKeyRange, DiscreteDomain.integers())
                .forEach(key -> compactionTestSender.send(message(valueOf(key), payloadPrefix + "-" + key)).join());
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
        return Files.list(Paths.get(System.getProperty("java.io.tmpdir")))
                .filter(p -> p.toFile().getName().startsWith("compaction-promo-compaction-test-snapshot-"))
                .collect(toList());
    }

}
