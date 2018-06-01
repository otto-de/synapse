package de.otto.synapse.eventsource.aws;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.s3.S3Service;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.aws.KinesisMessageLogReceiverEndpoint;
import de.otto.synapse.channel.aws.KinesisShardIterator;
import de.otto.synapse.channel.aws.KinesisStreamSetupUtils;
import de.otto.synapse.configuration.aws.TestMessageInterceptor;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.eventsource.DefaultEventSource;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.message.Message;
import de.otto.synapse.testsupport.TestStreamSource;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StopWatch;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.messagestore.MessageStores.emptyMessageStore;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(classes = KinesisEventSourceIntegrationTest.class)
public class KinesisEventSourceIntegrationTest {

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[]{});
    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET = 10;
    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET = 10;
    private static final int EXPECTED_NUMBER_OF_SHARDS = 2;
    private static final String TEST_CHANNEL = "synapse-test-channel";
    // from application-test.yml:
    private static final String INTEGRATION_TEST_BUCKET = "de-otto-promo-compaction-test-snapshots";

    @Autowired
    private KinesisClient kinesisClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private MessageInterceptorRegistry messageInterceptorRegistry;

    @Autowired
    private TestMessageInterceptor testMessageInterceptor;

    private EventSource integrationEventSource;

    private List<Message<String>> messages = synchronizedList(new ArrayList<>());

    @Before
    public void before() {
        messages.clear();
    }

    @PostConstruct
    public void setup() throws IOException {
        KinesisStreamSetupUtils.createStreamIfNotExists(kinesisClient, TEST_CHANNEL, EXPECTED_NUMBER_OF_SHARDS);
        deleteSnapshotFilesFromTemp();
        s3Service.createBucket(INTEGRATION_TEST_BUCKET);
        s3Service.deleteAllObjectsInBucket(INTEGRATION_TEST_BUCKET);

        /* We have to setup the EventSource manually, because otherwise the stream created above is not yet available
           when initializing it via @EnableEventSource
         */
        final KinesisMessageLogReceiverEndpoint kinesisMessageLog = new KinesisMessageLogReceiverEndpoint(TEST_CHANNEL, kinesisClient, objectMapper, null);
        kinesisMessageLog.registerInterceptorsFrom(messageInterceptorRegistry);
        this.integrationEventSource = new DefaultEventSource("integrationEventSource", emptyMessageStore(), kinesisMessageLog);
        this.integrationEventSource.register(MessageConsumer.of(".*", String.class, (message) -> messages.add(message)));
    }

    @Test
    public void consumeDataFromKinesisStream() {
        // when
        ChannelPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        integrationEventSource.consumeUntil(
                startFrom,
                now().plus(20, MILLIS)
        );

        assertThat(messages, not(empty()));
        assertThat(messages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET));
    }

    @Test
    public void registerInterceptorAndInterceptMessages() {
        // when
        testMessageInterceptor.clear();
        ChannelPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        integrationEventSource.consumeUntil(
                startFrom,
                now().plus(20, MILLIS)
        );

        final List<Message<String>> interceptedMessages = testMessageInterceptor.getInterceptedMessages();
        assertThat(interceptedMessages, not(empty()));
        assertThat(interceptedMessages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET));
    }

    @Test
    public void shouldStopEventSource() throws InterruptedException, ExecutionException, TimeoutException {
        try {
            // given
            ChannelPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

            // only fetch 2 records per iterator to be able to check against stop condition which is only evaluated after
            // retrieving new iterator
            setStaticFinalField(KinesisShardIterator.class, "FETCH_RECORDS_LIMIT", 2);

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            final CompletableFuture<ChannelPosition> completableFuture = supplyAsync(
                    () -> integrationEventSource.consume(startFrom),
                    executorService);

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
    public void consumeDeleteMessagesFromKinesisStream() {
        // given
        ChannelPosition startFrom = writeToStream("users_small1.txt").getLastStreamPosition();
        kinesisClient.putRecord(PutRecordRequest.builder().streamName(TEST_CHANNEL).partitionKey("deleteEvent").data(EMPTY_BYTE_BUFFER).build());
        // when
        integrationEventSource.consumeUntil(
                startFrom,
                now().plus(20, MILLIS)
        );
        // then
        assertThat(messages, hasSize(1));
        assertThat(messages.get(0).getKey(), is("deleteEvent"));
        assertThat(messages.get(0).getPayload(), is(nullValue()));
    }

    @Test
    public void consumerShouldReadNoMoreAfterStartingPoint() {
        // when
        writeToStream("users_small1.txt");
        ChannelPosition startFrom = writeToStream("users_small2.txt").getFirstReadPosition();

        // then
        ChannelPosition nextChannelPosition = integrationEventSource.consumeUntil(
                startFrom,
                now().plus(20, MILLIS)
        );

        assertThat(nextChannelPosition.shards(), hasSize(EXPECTED_NUMBER_OF_SHARDS));
        assertThat(messages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET));
        assertThat(messages.stream().map(Message::getKey).sorted().collect(Collectors.toList()), is(expectedListOfKeys()));
    }

    @Test
    public void consumerShouldResumeAtStartingPoint() {
        // when
        ChannelPosition startFrom = writeToStream("users_small1.txt").getLastStreamPosition();
        writeToStream("users_small2.txt");

        // then
        ChannelPosition next = integrationEventSource.consumeUntil(startFrom, now().plus(10, MILLIS));

        assertThat(messages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET + 2)); // +2 because of two Fake Records
        assertThat(next.shards(), hasSize(EXPECTED_NUMBER_OF_SHARDS));
    }

    private TestStreamSource writeToStream(String filename) {
        TestStreamSource streamSource = new TestStreamSource(kinesisClient, TEST_CHANNEL, filename);
        streamSource.writeToStream();
        return streamSource;
    }

    private List<String> expectedListOfKeys() {
        return IntStream.range(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET + 1, EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET + EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET + 1).mapToObj(String::valueOf).collect(Collectors.toList());
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
