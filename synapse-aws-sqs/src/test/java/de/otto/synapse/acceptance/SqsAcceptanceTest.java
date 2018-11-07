package de.otto.synapse.acceptance;

import de.otto.synapse.annotation.EnableMessageQueueReceiverEndpoint;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.annotation.MessageQueueConsumer;
import de.otto.synapse.channel.selector.MessageQueue;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Message;
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

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

import static de.otto.synapse.configuration.sqs.SqsTestConfiguration.SQS_INTEGRATION_TEST_CHANNEL;
import static de.otto.synapse.message.Message.message;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(
        basePackages = {"de.otto.synapse"})
@SpringBootTest(
        classes = SqsAcceptanceTest.class)
@EnableMessageQueueReceiverEndpoint(
        name = "sqsReceiver",
        channelName = SQS_INTEGRATION_TEST_CHANNEL)
@EnableMessageSenderEndpoint(
        name = "sqsSender",
        channelName = SQS_INTEGRATION_TEST_CHANNEL,
        selector = MessageQueue.class)
@DirtiesContext
public class SqsAcceptanceTest {

    private static final Logger LOG = getLogger(SqsAcceptanceTest.class);

    private final AtomicReference<Message<String>> lastSqsMessage = new AtomicReference<>(null);

    @Autowired
    private MessageSenderEndpoint sqsSender;

    @MessageQueueConsumer(endpointName = "sqsReceiver", payloadType = String.class)
    public void sqsConsumer(final Message<String> message) {
        LOG.info("Received message {} from SQS", message);
        lastSqsMessage.set(message);
    }

    @Before
    public void before() {
        lastSqsMessage.set(null);
    }

    @Test
    public void shouldSendAndReceiveSqsMessage() {
        final String expectedPayload = "some payload: " + LocalDateTime.now();
        sqsSender.send(message("test-key-shouldSendAndReceiveSqsMessage", expectedPayload)).join();
        await()
                .atMost(10, SECONDS)
                .until(() -> lastSqsMessage.get() != null && lastSqsMessage.get().getKey().equals("test-key-shouldSendAndReceiveSqsMessage") && expectedPayload.equals(lastSqsMessage.get().getPayload()));
    }
}
