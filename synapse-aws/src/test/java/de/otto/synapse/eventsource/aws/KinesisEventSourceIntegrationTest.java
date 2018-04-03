package de.otto.synapse.eventsource.aws;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.aws.KinesisMessageLog;
import de.otto.synapse.channel.aws.KinesisShardIterator;
import de.otto.synapse.channel.aws.KinesisStreamSetupUtils;
import de.otto.synapse.consumer.MessageConsumer;
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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.synchronizedList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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
    private static final String STREAM_NAME = "eventsourcing-synapse-test";

    @Autowired
    private KinesisClient kinesisClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    private EventSource eventSource;
    private List<Message<String>> messages = synchronizedList(new ArrayList<Message<String>>());

    @Before
    public void before() {
        messages.clear();
    }

    @PostConstruct
    public void setup() {
        KinesisStreamSetupUtils.createStreamIfNotExists(kinesisClient, STREAM_NAME, EXPECTED_NUMBER_OF_SHARDS);
        KinesisMessageLog kinesisMessageLog = new KinesisMessageLog(kinesisClient, STREAM_NAME);
        this.eventSource = new KinesisEventSource("kinesisEventSource", kinesisMessageLog, eventPublisher, objectMapper);
        this.eventSource.register(MessageConsumer.of(".*", String.class, (message) -> messages.add(message)));
    }

    @Test
    @Ignore("test consumption from actual kinesis productfeed stream - disable KinesisTestConfiguration and log in to aws to use real kinesis")
    public void shouldConsumeDataFromExistingStream() {
        EventSource productStreamEventSource = new KinesisEventSourceBuilder(objectMapper, eventPublisher, kinesisClient).buildEventSource("promo-productfeed-develop", "promo-productfeed-develop");
        productStreamEventSource.register(MessageConsumer.of(".*", String.class, (message) -> messages.add(message)));

        Predicate<Message<?>> stopAtDefinedInstant = (message) -> message.getHeader().getDurationBehind().get().getSeconds() < 10;

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        productStreamEventSource.consumeAll(ChannelPosition.fromHorizon(), stopAtDefinedInstant);
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());
    }

    @Test
    public void consumeDataFromKinesisStream() {
        // when
        ChannelPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        eventSource.consumeAll(
                startFrom,
                stopAfter(10)
        );

        assertThat(messages, not(empty()));
        assertThat(messages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET));
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
            final CompletableFuture<ChannelPosition> completableFuture = CompletableFuture.supplyAsync(() -> eventSource.consumeAll(
                    startFrom,
                    (message) -> false), executorService);

            Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> messages.size() > 0);

            // when
            eventSource.stop();

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
        kinesisClient.putRecord(PutRecordRequest.builder().streamName(STREAM_NAME).partitionKey("deleteEvent").data(EMPTY_BYTE_BUFFER).build());
        // when
        eventSource.consumeAll(
                startFrom,
                stopAfter(1)
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
        ChannelPosition nextChannelPosition = eventSource.consumeAll(
                startFrom,
                stopAfter(10));

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
        ChannelPosition next = eventSource.consumeAll(
                startFrom,
                (message) -> true);

        assertThat(messages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET + 2)); // +2 because of two Fake Records
        assertThat(next.shards(), hasSize(EXPECTED_NUMBER_OF_SHARDS));
    }

    private TestStreamSource writeToStream(String filename) {
        TestStreamSource streamSource = new TestStreamSource(kinesisClient, STREAM_NAME, filename);
        streamSource.writeToStream();
        return streamSource;
    }

    private List<String> expectedListOfKeys() {
        return IntStream.range(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET + 1, EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET + EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET + 1).mapToObj(String::valueOf).collect(Collectors.toList());
    }

    private Predicate<Message<?>> stopAfter(int n) {
        return (Message<?> message) -> (messages.size() == n || message.getHeader().getDurationBehind().get().isZero());
    }
}
