package de.otto.synapse.endpoint.receiver.sqs;


import de.otto.synapse.configuration.sqs.TestSqsMessageInterceptor;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.SqsClientHelper;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.testsupport.SqsTestStreamSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static de.otto.synapse.configuration.sqs.SqsTestConfiguration.SQS_INTEGRATION_TEST_CHANNEL;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedSet;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(classes = SqsMessageQueueIntegrationTest.class)
@DirtiesContext
public class SqsMessageQueueIntegrationTest {

    private static final Logger LOG = getLogger(SqsMessageQueueIntegrationTest.class);
    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET = 10;

    @Autowired
    private SqsAsyncClient sqsAsyncClient;

    @Autowired
    private MessageInterceptorRegistry messageInterceptorRegistry;

    @Autowired
    private TestSqsMessageInterceptor testSqsMessageInterceptor;

    private List<Message<String>> messages = synchronizedList(new ArrayList<>());
    private Set<String> threads = synchronizedSet(new HashSet<>());
    private SqsMessageQueueReceiverEndpoint sqsMessageQueue;

    @Before
    public void before() {
        messages.clear();
        new SqsClientHelper(sqsAsyncClient).purgeQueue(SQS_INTEGRATION_TEST_CHANNEL);
        sqsMessageQueue = new SqsMessageQueueReceiverEndpoint(SQS_INTEGRATION_TEST_CHANNEL, messageInterceptorRegistry, sqsAsyncClient, newSingleThreadExecutor(), null);
        sqsMessageQueue.register(MessageConsumer.of(".*", String.class, (message) -> {
            LOG.info("Consumed message {}", message.getKey());
            messages.add(message);
            threads.add(Thread.currentThread().getName());
        }));
    }

    @After
    public void after() {
        sqsMessageQueue.stop();
    }

    @Test
    public void consumeDataFromSqs() {
        // when
        writeToStream("users_small1.txt");

        // then
        sqsMessageQueue.consume();

        await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> messages.size() >= EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET);
        sqsMessageQueue.stop();

        assertThat(messages.get(0).getKey(), is(Key.of("some-message-0")));
        assertThat(messages.get(0).getHeader().getShardPosition(), is(Optional.empty()));
        assertThat(messages.get(0).getHeader().get("synapse_msg_key"), is("some-message-0"));
    }

    @Test
    public void registerInterceptorAndInterceptMessages() {
        // when
        testSqsMessageInterceptor.clear();
        writeToStream("users_small1.txt");

        // then
        sqsMessageQueue.consume();

        await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> testSqsMessageInterceptor.getInterceptedMessages().size() >= EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET);
    }

    private SqsTestStreamSource writeToStream(final String filename) {
        final SqsTestStreamSource streamSource = new SqsTestStreamSource(SQS_INTEGRATION_TEST_CHANNEL, filename);
        streamSource.writeToStream();
        return streamSource;
    }

}
