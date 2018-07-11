package de.otto.synapse.endpoint.receiver.aws;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.configuration.aws.TestMessageInterceptor;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Message;
import de.otto.synapse.testsupport.KinesisChannelSetupUtils;
import de.otto.synapse.testsupport.KinesisTestStreamSource;
import org.awaitility.Awaitility;
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
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedSet;
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
@SpringBootTest(classes = KinesisMessageLogReceiverEndpointIntegrationTest.class)
public class KinesisMessageLogReceiverEndpointIntegrationTest {

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[]{});
    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET = 10;
    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET = 10;
    private static final int EXPECTED_NUMBER_OF_SHARDS = 2;
    private static final String TEST_CHANNEL = "kinesis-ml-test-channel";

    @Autowired
    private KinesisClient kinesisClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MessageInterceptorRegistry messageInterceptorRegistry;

    @Autowired
    private TestMessageInterceptor testMessageInterceptor;

    private List<Message<String>> messages = synchronizedList(new ArrayList<>());
    private Set<String> threads = synchronizedSet(new HashSet<>());
    private KinesisMessageLogReceiverEndpoint kinesisMessageLog;

    @Before
    public void before() {
        messages.clear();
        /* We have to setup the EventSource manually, because otherwise the stream created above is not yet available
           when initializing it via @EnableEventSource
         */
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint(TEST_CHANNEL, kinesisClient, objectMapper, null);
        kinesisMessageLog.registerInterceptorsFrom(messageInterceptorRegistry);
        kinesisMessageLog.register(MessageConsumer.of(".*", String.class, (message) -> {
            messages.add(message);
            threads.add(Thread.currentThread().getName());
        }));

    }

    @After
    public void after() {
        kinesisMessageLog.stop();
    }

    @PostConstruct
    public void setup() throws IOException {
        KinesisChannelSetupUtils.createChannelIfNotExists(kinesisClient, TEST_CHANNEL, EXPECTED_NUMBER_OF_SHARDS);
    }

    @Test
    public void consumeDataFromKinesis() throws ExecutionException, InterruptedException {
        // when
        ChannelPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        kinesisMessageLog.consumeUntil(
                startFrom,
                now().plus(200, MILLIS)
        ).get();

        assertThat(messages, not(empty()));
        assertThat(messages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET));
    }

    @Test
    public void runInSeparateThreads() throws ExecutionException, InterruptedException {
        // when
        ChannelPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        kinesisMessageLog.consumeUntil(
                startFrom,
                now().plus(200, MILLIS)
        ).get();

        assertThat(threads, not(empty()));
        assertThat(threads, containsInAnyOrder("kinesis-message-log-0", "kinesis-message-log-1"));
    }

    @Test
    public void registerInterceptorAndInterceptMessages() throws ExecutionException, InterruptedException {
        // when
        testMessageInterceptor.clear();
        ChannelPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        kinesisMessageLog.consumeUntil(
                startFrom,
                now().plus(20, MILLIS)
        ).get();

        final List<Message<String>> interceptedMessages = testMessageInterceptor.getInterceptedMessages();
        assertThat(interceptedMessages, not(empty()));
        assertThat(interceptedMessages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET));
    }

    @Test
    public void shouldStopMessageLog() throws InterruptedException, ExecutionException, TimeoutException {
        try {
            // given
            final ChannelPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

            // only fetch 2 records per iterator to be able to check against stop condition which is only evaluated after
            // retrieving new iterator
            setStaticFinalField(KinesisShardIterator.class, "FETCH_RECORDS_LIMIT", 2);

            final CompletableFuture<ChannelPosition> completableFuture = kinesisMessageLog.consume(startFrom);

            Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> messages.size() > 0);

            // when
            kinesisMessageLog.stop();

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
    public void consumeDeleteMessagesFromKinesis() throws ExecutionException, InterruptedException {
        // given
        final ChannelPosition startFrom = writeToStream("users_small1.txt").getLastStreamPosition();
        kinesisClient.putRecord(PutRecordRequest.builder().streamName(TEST_CHANNEL).partitionKey("deleteEvent").data(EMPTY_BYTE_BUFFER).build());
        // when
        kinesisMessageLog.consumeUntil(
                startFrom,
                now().plus(20, MILLIS)
        ).get();

        // then
        assertThat(messages, hasSize(greaterThanOrEqualTo(1)));
        assertThat(messages.get(messages.size()-1).getKey(), is("deleteEvent"));
        assertThat(messages.get(messages.size()-1).getPayload(), is(nullValue()));
    }

    @Test
    public void consumerShouldReadNoMoreAfterStartingPoint() throws ExecutionException, InterruptedException {
        // when
        writeToStream("users_small1.txt");
        ChannelPosition startFrom = writeToStream("users_small2.txt").getFirstReadPosition();

        // then
        ChannelPosition nextChannelPosition = kinesisMessageLog.consumeUntil(
                startFrom,
                now().plus(20, MILLIS)
        ).get();

        assertThat(nextChannelPosition.shards(), hasSize(EXPECTED_NUMBER_OF_SHARDS));
        assertThat(messages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET));
        assertThat(messages.stream().map(Message::getKey).sorted().collect(Collectors.toList()), is(expectedListOfKeys()));
    }

    @Test
    public void consumerShouldResumeAtStartingPoint() throws ExecutionException, InterruptedException {
        // when
        ChannelPosition startFrom = writeToStream("users_small1.txt").getLastStreamPosition();
        writeToStream("users_small2.txt");

        // then
        ChannelPosition next = kinesisMessageLog.consumeUntil(startFrom, now().plus(10, MILLIS)).get();

        assertThat(messages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET + 2)); // +2 because of two Fake Records
        assertThat(next.shards(), hasSize(EXPECTED_NUMBER_OF_SHARDS));
    }

    private KinesisTestStreamSource writeToStream(String filename) {
        KinesisTestStreamSource streamSource = new KinesisTestStreamSource(kinesisClient, TEST_CHANNEL, filename);
        streamSource.writeToStream();
        return streamSource;
    }

    private List<String> expectedListOfKeys() {
        return IntStream.range(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET + 1, EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET + EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET + 1).mapToObj(String::valueOf).collect(Collectors.toList());
    }

}
