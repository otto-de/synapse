package de.otto.synapse.eventsource.aws;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.configuration.aws.TestMessageInterceptor;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.aws.KinesisMessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.aws.KinesisShardIterator;
import de.otto.synapse.eventsource.DefaultEventSource;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.helper.s3.S3Helper;
import de.otto.synapse.message.Message;
import de.otto.synapse.testsupport.KinesisChannelSetupUtils;
import de.otto.synapse.testsupport.KinesisTestStreamSource;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.s3.S3Client;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static de.otto.synapse.messagestore.MessageStores.emptyMessageStore;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.synchronizedList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(classes = KinesisEventSourceIntegrationTest.class)
public class KinesisEventSourceIntegrationTest {

    private static final Logger LOG = getLogger(KinesisEventSourceIntegrationTest.class);

    private static final SdkBytes EMPTY_BYTE_BUFFER = SdkBytes.fromByteArray(new byte[]{});
    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET = 10;
    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET = 10;
    private static final int EXPECTED_NUMBER_OF_SHARDS = 1;
    private static final String TEST_CHANNEL = "kinesis-es-test-channel";
    // from application-test.yml:
    private static final String INTEGRATION_TEST_BUCKET = "de-otto-promo-compaction-test-snapshots";

    @Autowired
    private KinesisAsyncClient kinesisClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @Autowired
    private S3Client s3Client;

    private S3Helper s3Helper;

    @Autowired
    private MessageInterceptorRegistry messageInterceptorRegistry;

    @Autowired
    private TestMessageInterceptor testMessageInterceptor;

    private EventSource integrationEventSource;

    private List<Message<String>> messages = synchronizedList(new ArrayList<>());

    @PostConstruct
    public void setup() throws IOException {
        KinesisChannelSetupUtils.createChannelIfNotExists(kinesisClient, TEST_CHANNEL, EXPECTED_NUMBER_OF_SHARDS);
        deleteSnapshotFilesFromTemp();
        s3Helper = new S3Helper(s3Client);
        s3Helper.createBucket(INTEGRATION_TEST_BUCKET);
        s3Helper.deleteAllObjectsInBucket(INTEGRATION_TEST_BUCKET);
    }

    @Before
    public void before() {
        messages.clear();

        /* We have to setup the EventSource manually, because otherwise the stream created above is not yet available
           when initializing it via @EnableEventSource
         */
        final KinesisMessageLogReceiverEndpoint kinesisMessageLog = new KinesisMessageLogReceiverEndpoint(TEST_CHANNEL, kinesisClient, objectMapper, null);
        kinesisMessageLog.registerInterceptorsFrom(messageInterceptorRegistry);
        this.integrationEventSource = new DefaultEventSource(emptyMessageStore(), kinesisMessageLog);
        this.integrationEventSource.register(MessageConsumer.of(".*", String.class, (message) -> messages.add(message)));
    }

    @After
    public void after() {
        integrationEventSource.stop();
    }

    @Test
    public void consumeDataFromKinesisStream() throws ExecutionException, InterruptedException {
        // when
        ChannelPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        integrationEventSource.consumeUntil(
                now().plus(20, MILLIS)
        ).get();

        assertThat(messages, not(empty()));
        assertThat(messages, hasSize(greaterThanOrEqualTo(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET)));
    }

    @Test
    public void registerInterceptorAndInterceptMessages() throws ExecutionException, InterruptedException {
        // when
        testMessageInterceptor.clear();
        ChannelPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        integrationEventSource.consumeUntil(
                now().plus(20, MILLIS)
        ).get();

        final List<Message<String>> interceptedMessages = testMessageInterceptor.getInterceptedMessages();
        assertThat(interceptedMessages, not(empty()));
        assertThat(interceptedMessages, hasSize(greaterThanOrEqualTo(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET)));
    }

    @Test
    public void shouldStopEventSource() throws InterruptedException, ExecutionException, TimeoutException {
        try {
            // given
            writeToStream("users_small1.txt");

            // only fetch 2 records per iterator to be able to check against stop condition which is only evaluated after
            // retrieving new iterator
            setStaticFinalField(KinesisShardIterator.class, "FETCH_RECORDS_LIMIT", 2);

            final CompletableFuture<ChannelPosition> completableFuture = integrationEventSource.consume();

            Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> messages.size() > 0);

            // when
            integrationEventSource.stop();

            // then
            assertThat(completableFuture.get(2L, TimeUnit.SECONDS), is(notNullValue()));
            assertThat(completableFuture.isDone(), is(true));
            assertThat(messages.size(), lessThan(10));
        } finally {
            setStaticFinalField(KinesisShardIterator.class, "FETCH_RECORDS_LIMIT", 10000);
        }
    }

    private void setStaticFinalField(Class<?> clazz, String fieldName, Object value) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            Field modifiers = Field.class.getDeclaredField("modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void consumeDeleteMessagesFromKinesisStream() throws ExecutionException, InterruptedException {
        // given
        writeToStream("users_small1.txt");
        final String partitionKey = "deleteEvent-"+UUID.randomUUID().toString();
        kinesisClient.putRecord(PutRecordRequest.builder().streamName(TEST_CHANNEL).partitionKey(partitionKey).data(EMPTY_BYTE_BUFFER).build());
        // when
        integrationEventSource.consumeUntil(
                now().plus(500, MILLIS)
        ).get();

        // then
        assertThat(messages, hasSize(greaterThanOrEqualTo(1)));
        final Message<String> message = messages.get(messages.size() - 1);
        assertThat(message.getKey(), is(partitionKey));
        assertThat(message.getPayload(), is(nullValue()));
    }

    private KinesisTestStreamSource writeToStream(String filename) {
        KinesisTestStreamSource streamSource = new KinesisTestStreamSource(kinesisClient, TEST_CHANNEL, filename);
        streamSource.writeToStream();
        return streamSource;
    }

    private void deleteSnapshotFilesFromTemp() throws IOException {
        getSnapshotFilePaths()
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        LOG.error("Error deleting files from {}: {}", path, e.getMessage());
                    }
                });
    }

    private List<Path> getSnapshotFilePaths() throws IOException {
        return Files.list(Paths.get(System.getProperty("java.io.tmpdir")))
                .filter(p -> p.toFile().getName().startsWith("compaction-promo-compaction-test-snapshot-"))
                .collect(Collectors.toList());
    }
}
