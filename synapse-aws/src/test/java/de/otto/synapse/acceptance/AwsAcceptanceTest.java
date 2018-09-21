package de.otto.synapse.acceptance;

import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.annotation.messagequeue.EnableMessageQueueReceiverEndpoint;
import de.otto.synapse.annotation.messagequeue.EnableMessageQueueSenderEndpoint;
import de.otto.synapse.annotation.messagequeue.MessageQueueConsumer;
import de.otto.synapse.configuration.aws.KinesisTestConfiguration;
import de.otto.synapse.configuration.aws.SqsTestConfiguration;
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.*;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(properties = "synapse.snapshot.bucketName=de-otto-integrationtest-snapshots", classes = AwsAcceptanceTest.class)
@EnableEventSource(name = "integrationTestEventSource", channelName = KinesisTestConfiguration.KINESIS_INTEGRATION_TEST_CHANNEL)
@EnableMessageQueueReceiverEndpoint(name = "sqsReceiver", channelName = SqsTestConfiguration.SQS_INTEGRATION_TEST_CHANNEL)
@EnableMessageQueueSenderEndpoint(name = "sqsSender", channelName = SqsTestConfiguration.SQS_INTEGRATION_TEST_CHANNEL)
@DirtiesContext
public class AwsAcceptanceTest {

    private static final Logger LOG = getLogger(AwsAcceptanceTest.class);

    private final AtomicReference<Message<String>> lastSqsMessage = new AtomicReference<>(null);

    @Autowired
    private MessageSenderEndpoint sqsSender;

    @EventSourceConsumer(eventSource = "integrationTestEventSource", payloadType = String.class)
    public void consumer(final Message<String> message) {
        LOG.info("Received message {} from Kinesis", message);
    }

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
        sqsSender.send(Message.message("test", expectedPayload));
        await()
                .atMost(1, SECONDS)
                .until(() -> lastSqsMessage.get() != null && expectedPayload.equals(lastSqsMessage.get().getPayload()));
    }
}
