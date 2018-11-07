package de.otto.synapse.acceptance;

import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Message;
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

import static de.otto.synapse.configuration.kinesis.KinesisTestConfiguration.KINESIS_INTEGRATION_TEST_CHANNEL;
import static de.otto.synapse.message.Message.message;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(
        basePackages = {"de.otto.synapse"})
@SpringBootTest(
        properties = "synapse.snapshot.bucketName=de-otto-integrationtest-snapshots",
        classes = KinesisAcceptanceTest.class)
@EnableEventSource(
        name = "kinesisEventSource",
        channelName = KINESIS_INTEGRATION_TEST_CHANNEL)
@EnableMessageSenderEndpoint(
        name = "kinesisSender",
        channelName = KINESIS_INTEGRATION_TEST_CHANNEL)
@DirtiesContext
public class KinesisAcceptanceTest {

    private static final Logger LOG = getLogger(KinesisAcceptanceTest.class);

    private final AtomicReference<Message<String>> lastEventSourceMessage = new AtomicReference<>(null);

    @Autowired
    private MessageSenderEndpoint kinesisSender;

    @Autowired
    private MessageLogReceiverEndpoint kinesisMessageLogReceiverEndpoint;


    @EventSourceConsumer(eventSource = "kinesisEventSource", payloadType = String.class)
    public void eventSourceConsumer(final Message<String> message) {
        LOG.info("Received message {} from EventSource", message);
        lastEventSourceMessage.set(message);
    }

    @Test
    public void shouldRegisterMessageLogReceiverEndpoint() {
        assertThat(kinesisMessageLogReceiverEndpoint.getChannelName())
                .isEqualTo(KINESIS_INTEGRATION_TEST_CHANNEL);
    }

    @Test
    public void shouldSendAndReceiveKinesisMessage() {
        final String expectedPayload = "some payload: " + LocalDateTime.now();
        kinesisSender.send(message("test-key-shouldSendAndReceiveKinesisMessage", expectedPayload)).join();
        await()
                .atMost(10, SECONDS)
                .until(() -> lastEventSourceMessage.get() != null && lastEventSourceMessage.get().getKey().equals("test-key-shouldSendAndReceiveKinesisMessage") && expectedPayload.equals(lastEventSourceMessage.get().getPayload()));
    }

}
