package de.otto.synapse.acceptance;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Header;
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

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

import static de.otto.synapse.configuration.kinesis.KinesisTestConfiguration.KINESIS_INTEGRATION_TEST_CHANNEL;
import static de.otto.synapse.endpoint.DefaultSenderHeadersInterceptor.*;
import static de.otto.synapse.message.Header.requestHeader;
import static de.otto.synapse.message.Message.message;
import static java.time.Instant.now;
import static java.time.Instant.ofEpochSecond;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
    private MessageSenderEndpoint kinesisV1Sender;

    @Autowired
    private MessageSenderEndpoint kinesisV2Sender;

    @Autowired
    private MessageLogReceiverEndpoint kinesisMessageLogReceiverEndpoint;


    @EventSourceConsumer(eventSource = "kinesisEventSource", payloadType = String.class)
    public void eventSourceConsumer(final Message<String> message) {
        LOG.info("Received message {} from EventSource", message);
        lastEventSourceMessage.set(message);
    }

    @Test
    public void shouldRegisterMessageLogReceiverEndpoint() {
        assertThat(kinesisMessageLogReceiverEndpoint.getChannelName(), is(KINESIS_INTEGRATION_TEST_CHANNEL));
    }

    @Test
    public void shouldSendAndReceiveKinesisMessage() {
        final String expectedPayload = "some payload: " + LocalDateTime.now();
        kinesisSender.send(message("test-key-shouldSendAndReceiveKinesisMessage", expectedPayload)).join();
        await()
                .atMost(10, SECONDS)
                .until(() -> lastEventSourceMessage.get() != null && lastEventSourceMessage.get().getKey().equals("test-key-shouldSendAndReceiveKinesisMessage") && expectedPayload.equals(lastEventSourceMessage.get().getPayload()));
    }

    @Test
    // TODO: V1 format as default format is subject to change in 0.13.0 or later
    public void shouldSendAndReceiveKinesisMessageInDefaultFormat() {
        kinesisSender.send(message("test-key-shouldSendAndReceiveKinesisMessageInDefaultFormat", "")).join();
        await()
                .atMost(10, SECONDS)
                .until(() -> lastEventSourceMessage.get() != null && lastEventSourceMessage.get().getKey().equals("test-key-shouldSendAndReceiveKinesisMessageInDefaultFormat"));

        final ImmutableMap<String, String> attributes = lastEventSourceMessage.get().getHeader().getAttributes();
        assertThat(attributes, is(emptyMap()));
    }

    @Test
    public void shouldSendAndReceiveKinesisMessageInV1Format() {
        kinesisV1Sender.send(
                message("test-key-shouldSendAndReceiveKinesisMessageInV1Format","no special payload"))
                .join();
        await()
                .atMost(10, SECONDS)
                .until(() -> lastEventSourceMessage.get() != null && lastEventSourceMessage.get().getKey().equals("test-key-shouldSendAndReceiveKinesisMessageInV1Format"));

        final ImmutableMap<String, String> attributes = lastEventSourceMessage.get().getHeader().getAttributes();
        assertThat(attributes, is(emptyMap()));
    }

    @Test
    public void shouldSendAndReceiveKinesisMessageInV2FormatWithDefaultHeaders() {
        final Instant started = now();
        kinesisV2Sender.send(message("test-key-shouldSendAndReceiveKinesisMessageWithDefaultHeaders", "{}")).join();
        await()
                .atMost(10, SECONDS)
                .until(() ->
                        lastEventSourceMessage.get() != null
                                && lastEventSourceMessage.get().getKey().equals("test-key-shouldSendAndReceiveKinesisMessageWithDefaultHeaders")
                                && lastEventSourceMessage.get().getHeader().getArrivalTimestamp().isAfter(started));

        final ImmutableMap<String, String> attributes = lastEventSourceMessage.get().getHeader().getAttributes();
        assertThat(attributes, hasEntry(equalTo(MSG_ID_ATTR), not(isEmptyOrNullString())));
        assertThat(attributes, hasEntry(equalTo(MSG_TIMESTAMP_ATTR), not(isEmptyOrNullString())));
        assertThat(attributes, hasEntry(MSG_SENDER_ATTR, "Synapse"));
        assertThat(lastEventSourceMessage.get().getPayload(), is("{}"));
    }

    @Test
    public void shouldSendAndReceiveKinesisMessageInV2FormatWithCustomHeaders() {
        final Header header = requestHeader(ImmutableMap.of(
                "string", "some value",
                "timestamp", ofEpochSecond(42).toString())
        );
        kinesisV2Sender.send(message("test-key-shouldSendAndReceiveKinesisMessageWithCustomHeaders", header, "")).join();
        await()
                .atMost(10, SECONDS)
                .until(() -> lastEventSourceMessage.get() != null && lastEventSourceMessage.get().getKey().equals("test-key-shouldSendAndReceiveKinesisMessageWithCustomHeaders"));
        final ImmutableMap<String, String> attributes = lastEventSourceMessage.get().getHeader().getAttributes();
        assertThat(attributes, hasEntry("string", "some value"));
        assertThat(attributes, hasEntry("timestamp", "1970-01-01T00:00:42Z"));
    }


}
