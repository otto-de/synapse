package de.otto.synapse.acceptance;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.SimpleKey;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static de.otto.synapse.configuration.kinesis.KinesisTestConfiguration.KINESIS_INTEGRATION_TEST_CHANNEL;
import static de.otto.synapse.message.DefaultHeaderAttr.*;
import static de.otto.synapse.message.Header.of;
import static de.otto.synapse.message.Key.of;
import static de.otto.synapse.message.Message.copyOf;
import static de.otto.synapse.message.Message.message;
import static java.time.Instant.ofEpochSecond;
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
        properties = {
                "synapse.snapshot.bucketName=de-otto-integrationtest-snapshots",
                "spring.main.allow-bean-definition-overriding=true"
        },
        classes = KinesisAcceptanceTest.class)
@EnableEventSource(
        name = "kinesisEventSource",
        channelName = KINESIS_INTEGRATION_TEST_CHANNEL)
@EnableMessageSenderEndpoint(
        name = "kinesisSender",
        channelName = KINESIS_INTEGRATION_TEST_CHANNEL,
        selector = MessageLog.class)
@DirtiesContext
public class KinesisAcceptanceTest {

    private static final Logger LOG = getLogger(KinesisAcceptanceTest.class);

    private final ConcurrentMap<String, Message<String>> receivedMessages = new ConcurrentHashMap<>();

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
        receivedMessages.put(message.getKey().partitionKey(), message);
    }

    @Test
    public void shouldRegisterMessageLogReceiverEndpoint() {
        assertThat(kinesisMessageLogReceiverEndpoint.getChannelName(), is(KINESIS_INTEGRATION_TEST_CHANNEL));
    }

    @Test
    public void shouldSendAndReceiveV2KinesisMessageWithCompoundKey() {
        final Key compoundKey = of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final Message<String> receivedMessage = sendMessageAndAwait(message(compoundKey, ""), kinesisV2Sender);

        assertThat(receivedMessage.getKey(), is(compoundKey));
    }

    @Test
    // TODO: V1 format as default format is subject to change in 0.15.0 or later
    public void shouldSendAndReceiveKinesisMessageInDefaultFormat() {
        final Message<String> receivedMessage = sendMessageAndAwait(message("", ""), kinesisSender);

        final ImmutableMap<String, String> attributes = receivedMessage.getHeader().getAll();
        assertThat(attributes.keySet(), contains(MSG_ARRIVAL_TS.key(), MSG_RECEIVER_TS.key()));
    }

    @Test
    public void shouldSendAndReceiveKinesisMessageInV1Format() {
        final Message<String> receivedMessage = sendMessageAndAwait(message("", ""), kinesisV1Sender);
        final ImmutableMap<String, String> attributes = receivedMessage.getHeader().getAll();
        assertThat(attributes.keySet(), contains(MSG_ARRIVAL_TS.key(), MSG_RECEIVER_TS.key()));
    }

    @Test
    public void shouldSendAndReceiveKinesisMessageInV2FormatWithDefaultHeaders() {
        final Message<String> receivedMessage = sendMessageAndAwait(message("", "{}"), kinesisV2Sender);

        final ImmutableMap<String, String> attributes = receivedMessage.getHeader().getAll();
        assertThat(attributes, hasEntry(equalTo(MSG_ID.key()), not(isEmptyOrNullString())));
        assertThat(attributes, hasEntry(equalTo(MSG_SENDER_TS.key()), not(isEmptyOrNullString())));
        assertThat(attributes, hasEntry(equalTo(MSG_ARRIVAL_TS.key()), not(isEmptyOrNullString())));
        assertThat(attributes, hasEntry(equalTo(MSG_RECEIVER_TS.key()), not(isEmptyOrNullString())));
        assertThat(attributes, hasEntry(MSG_SENDER.key(), "Synapse"));
        assertThat(receivedMessage.getPayload(), is("{}"));
        assertThat(receivedMessage.getKey(), is(instanceOf(SimpleKey.class)));
        final Instant sent = receivedMessage.getHeader().getAsInstant(MSG_SENDER_TS);
        final Instant arrived = receivedMessage.getHeader().getAsInstant(MSG_ARRIVAL_TS);
        final Instant received = receivedMessage.getHeader().getAsInstant(MSG_RECEIVER_TS);
        assertThat(sent.isAfter(arrived), is(false));
        assertThat(arrived.isAfter(received), is(false));
    }

    @Test
    public void shouldSendAndReceiveKinesisMessageInV2FormatWithCustomHeaders() {
        final Header header = of(ImmutableMap.of(
                "string", "some value",
                "timestamp", ofEpochSecond(42).toString())
        );
        final Message<?> receivedMessage = sendMessageAndAwait(message("", header, ""), kinesisV2Sender);
        final ImmutableMap<String, String> attributes = receivedMessage.getHeader().getAll();
        assertThat(attributes, hasEntry("string", "some value"));
        assertThat(attributes, hasEntry("timestamp", "1970-01-01T00:00:42Z"));
    }


    private Message<String> sendMessageAndAwait(final Message<String> message,
                                                final MessageSenderEndpoint kinesisSender) {
        final AtomicReference<Message<String>> result = new AtomicReference<>();
        final String messageId = "urn:message:kinesis:" + UUID.randomUUID().toString();
        kinesisSender
                .send(copyOf(message)
                        .withKey(message.getKey() instanceof SimpleKey ? of(messageId) : of(messageId, message.getKey().compactionKey()))
                        .build())
                .join();
        await()
                .atMost(30, SECONDS)
                .until(() -> {
                    final Message<String> msg = receivedMessages.get(messageId);
                    if (msg != null && msg.getKey().partitionKey().equals(messageId)) {
                        result.set(copyOf(msg).withKey(message.getKey()).build());
                        return true;
                    }
                    return false;
                });
        return result.get();
    }
}
