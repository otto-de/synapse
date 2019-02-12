package de.otto.synapse.endpoint.receiver.kinesis;


import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.selector.Kinesis;
import de.otto.synapse.configuration.kinesis.TestMessageInterceptor;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.StopCondition.endOfChannel;
import static de.otto.synapse.configuration.kinesis.KinesisTestConfiguration.EXPECTED_NUMBER_OF_SHARDS;
import static de.otto.synapse.configuration.kinesis.KinesisTestConfiguration.KINESIS_INTEGRATION_TEST_CHANNEL;
import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;
import static java.lang.Thread.sleep;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedSet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
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
@EnableMessageSenderEndpoint(name = "kinesisSender", channelName = KINESIS_INTEGRATION_TEST_CHANNEL, selector = Kinesis.class)
@DirtiesContext
public class KinesisMessageLogReceiverEndpointIntegrationTest {

    @Autowired
    private KinesisAsyncClient kinesisAsyncClient;

    @Autowired
    private MessageInterceptorRegistry messageInterceptorRegistry;

    @Autowired
    private TestMessageInterceptor testMessageInterceptor;

    @Autowired
    private MessageSenderEndpoint kinesisSender;

    @Autowired
    private ExecutorService kinesisMessageLogExecutorService;

    private List<Message<String>> messages = synchronizedList(new ArrayList<>());
    private Set<String> threads = synchronizedSet(new HashSet<>());
    private KinesisMessageLogReceiverEndpoint kinesisMessageLog;

    @Before
    public void before() {
        /* We have to setup the EventSource manually, because otherwise the stream created above is not yet available
           when initializing it via @EnableEventSource
         */
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint(KINESIS_INTEGRATION_TEST_CHANNEL, messageInterceptorRegistry, kinesisAsyncClient, kinesisMessageLogExecutorService, null);
        kinesisMessageLog.register(MessageConsumer.of(".*", String.class, (message) -> {
            messages.add(message);
            threads.add(Thread.currentThread().getName());
        }));

    }

    @After
    public void after() {
        kinesisMessageLog.stop();
    }

    @Test
    public void consumeDataFromKinesis() throws ExecutionException, InterruptedException {
        // given
        final ChannelPosition startFrom = findCurrentPosition();
        sendTestMessages(Range.closed(1, 10), "some payload");

        // when
        kinesisMessageLog.consumeUntil(
                startFrom,
                endOfChannel()
        ).get();

        // then
        assertThat(messages, not(empty()));
        assertThat(messages, hasSize(10));
    }

    @Test
    public void runInSeparateThreads() throws ExecutionException, InterruptedException {
        // when
        final ChannelPosition startFrom = findCurrentPosition();
        sendTestMessages(Range.closed(1, 10), "some payload");

        // then
        kinesisMessageLog
                .consumeUntil(startFrom, endOfChannel())
                .get();

        assertThat(threads, hasSize(2));
    }

    @Test
    public void shouldDisposeThreadsAfterConsumption() throws ExecutionException, InterruptedException {
        // when
        final ChannelPosition startFrom = findCurrentPosition();
        sendTestMessages(Range.closed(1, 10), "some payload");

        // then
        kinesisMessageLog
                .consumeUntil(startFrom, endOfChannel())
                .get();
        final List<String> threadNamesBefore = Thread.getAllStackTraces()
                .keySet()
                .stream()
                .map(Thread::getName)
                .filter((name)->name.startsWith("kinesis-message-log-"))
                .collect(toList());
        kinesisMessageLog
                .consumeUntil(startFrom, endOfChannel())
                .get();
        final List<String> threadNamesAfter = Thread.getAllStackTraces()
                .keySet()
                .stream()
                .map(Thread::getName)
                .filter((name)->name.startsWith("kinesis-message-log-"))
                .collect(toList());
        assertThat(threadNamesBefore, is(threadNamesAfter));
    }

    @Test
    public void registerInterceptorAndInterceptMessages() throws ExecutionException, InterruptedException {
        // when
        final ChannelPosition startFrom = findCurrentPosition();
        sendTestMessages(Range.closed(1, 10), "some payload");

        // then
        kinesisMessageLog
                .consumeUntil(startFrom, endOfChannel())
                .get();

        final List<Message<String>> interceptedMessages = testMessageInterceptor.getInterceptedMessages();
        assertThat(interceptedMessages, not(empty()));
    }

    @Test
    public void shouldStopMessageLog() throws InterruptedException, ExecutionException, TimeoutException {
        try {
            // given
            final ChannelPosition startFrom = findCurrentPosition();
            sendTestMessages(Range.closed(1, 10), "some payload");

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

    @Test
    public void consumeDeleteMessagesFromKinesis() throws ExecutionException, InterruptedException {
        // given
        final ChannelPosition startFrom = findCurrentPosition();
        kinesisSender.send(message("deletedMessage", null)).join();

        // when
        kinesisMessageLog
                .consumeUntil(startFrom, endOfChannel())
                .get();

        // then
        assertThat(messages, hasSize(1));
        assertThat(messages.get(0).getKey(), is(Key.of("deletedMessage")));
        assertThat(messages.get(0).getPayload(), is(nullValue()));
    }

    @Test
    public void consumerShouldResumeAtStartingPoint() throws ExecutionException, InterruptedException {
        // when
        sendTestMessages(Range.closed(1, 10), "some payload");
        final ChannelPosition startFrom = findCurrentPosition();
        sendTestMessages(Range.closed(11, 15), "some other payload");

        // then
        ChannelPosition next = kinesisMessageLog
                .consumeUntil(startFrom, endOfChannel())
                .get();

        assertThat(messages, hasSize(5));
        assertThat(next.shards(), hasSize(EXPECTED_NUMBER_OF_SHARDS));
        assertThat(messages.stream().map(Message::getKey).map(Key::toString).sorted().collect(toList()), contains("11", "12", "13", "14", "15"));
    }

    private ChannelPosition findCurrentPosition() throws InterruptedException {
        sleep(50);
        final ChannelPosition channelPosition = kinesisMessageLog
                .consumeUntil(fromHorizon(), endOfChannel())
                .join();
        threads.clear();
        messages.clear();
        testMessageInterceptor.clear();
        return channelPosition;
    }

    private void sendTestMessages(final Range<Integer> messageKeyRange,
                                  final String payloadPrefix) throws InterruptedException {
        ContiguousSet.create(messageKeyRange, DiscreteDomain.integers())
                .forEach(key -> kinesisSender.send(message(valueOf(key), payloadPrefix + "-" + key)).join());
        sleep(20);
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
}
