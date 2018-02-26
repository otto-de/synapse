package de.otto.edison.eventsourcing.aws.kinesis;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.aws.testsupport.TestStreamSource;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.MessageConsumer;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.message.Message;
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
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.synchronizedList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.edison.eventsourcing"})
@SpringBootTest(classes = KinesisEventSourceIntegrationTest.class)
public class KinesisEventSourceIntegrationTest {

    private static final Logger LOG = getLogger(KinesisEventSourceIntegrationTest.class);

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[]{});
    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET = 10;
    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET = 10;
    private static final int EXPECTED_NUMBER_OF_SHARDS = 2;
    private static final String STREAM_NAME = "promo-compaction-test";

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
        KinesisStream kinesisStream = new KinesisStream(kinesisClient, STREAM_NAME);
        this.eventSource = new KinesisEventSource("kinesisEventSource", kinesisStream, eventPublisher, objectMapper);
        this.eventSource.register(MessageConsumer.of(".*", String.class, (message) -> messages.add(message)));
    }

    @Test
    public void consumeDataFromKinesisStream() {
        // when
        StreamPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        eventSource.consumeAll(
                startFrom,
                stopAfter(10)
        );

        assertThat(messages, not(empty()));
        assertThat(messages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET));
    }

    @Test
    public void shouldStopEventSource() throws InterruptedException, ExecutionException {
        // when
        StreamPosition startFrom = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        ExecutorService exec = Executors.newSingleThreadExecutor();
        final CompletableFuture<StreamPosition> completableFuture = CompletableFuture.supplyAsync(() -> eventSource.consumeAll(
                startFrom,
                (message) -> false), exec);

        eventSource.stop();
        exec.awaitTermination(2, TimeUnit.SECONDS);
        assertThat(completableFuture.isDone(), is(true));
        assertThat(completableFuture.get(), is(notNullValue()));
    }

    @Test
    public void consumeDeleteMessagesFromKinesisStream() {
        // given
        StreamPosition startFrom = writeToStream("users_small1.txt").getLastStreamPosition();
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
        StreamPosition startFrom = writeToStream("users_small2.txt").getFirstReadPosition();

        // then
        StreamPosition nextStreamPosition = eventSource.consumeAll(
                startFrom,
                stopAfter(10));

        assertThat(nextStreamPosition.shards(), hasSize(EXPECTED_NUMBER_OF_SHARDS));
        assertThat(messages, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET));
        assertThat(messages.stream().map(Message::getKey).sorted().collect(Collectors.toList()), is(expectedListOfKeys()));
    }

    @Test
    public void consumerShouldResumeAtStartingPoint() {
        // when
        StreamPosition startFrom = writeToStream("users_small1.txt").getLastStreamPosition();
        writeToStream("users_small2.txt");

        // then
        StreamPosition next = eventSource.consumeAll(
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
        return (Message<?> message) -> {
            return messages.size() == n;
        };
    }
}
