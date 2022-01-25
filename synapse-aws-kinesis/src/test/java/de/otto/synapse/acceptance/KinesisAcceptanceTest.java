package de.otto.synapse.acceptance;

import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static de.otto.synapse.acceptance.KinesisAcceptanceTest.SomePayload.somePayload;
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

    private final ConcurrentMap<String, Message<SomePayload>> receivedMessages = new ConcurrentHashMap<>();

    @Autowired
    private MessageSenderEndpoint kinesisSender;

    @Autowired
    private MessageSenderEndpoint kinesisV1Sender;

    @Autowired
    private MessageSenderEndpoint kinesisV2Sender;

    @Autowired
    private MessageLogReceiverEndpoint kinesisMessageLogReceiverEndpoint;


    @EventSourceConsumer(
            eventSource = "kinesisEventSource",
            payloadType = SomePayload.class)
    public void eventSourceConsumer(final Message<SomePayload> message) {
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
        final Message<SomePayload> receivedMessage = sendMessageAndAwait(message(compoundKey, somePayload("")), kinesisV2Sender);

        assertThat(receivedMessage.getKey(), is(compoundKey));
    }

    @Test
    // TODO: V1 format as default format is subject to change in 0.15.0 or later
    public void shouldSendAndReceiveKinesisMessageInDefaultFormat() {
        final Message<SomePayload> receivedMessage = sendMessageAndAwait(message("", somePayload("")), kinesisSender);

        final ImmutableMap<String, String> attributes = receivedMessage.getHeader().getAll();
        assertThat(attributes.keySet(), contains(MSG_ARRIVAL_TS.key(), MSG_RECEIVER_TS.key()));
    }

    @Test
    public void shouldSendAndReceiveKinesisMessageInV1Format() {
        final Message<SomePayload> receivedMessage = sendMessageAndAwait(message("", somePayload("")), kinesisV1Sender);
        final ImmutableMap<String, String> attributes = receivedMessage.getHeader().getAll();
        assertThat(attributes.keySet(), contains(MSG_ARRIVAL_TS.key(), MSG_RECEIVER_TS.key()));
    }

    @Test
    public void shouldSendAndReceiveKinesisMessageInV2FormatWithDefaultHeaders() {
        final Message<SomePayload> receivedMessage = sendMessageAndAwait(message("", somePayload("foo")), kinesisV2Sender);

        final ImmutableMap<String, String> attributes = receivedMessage.getHeader().getAll();
        assertThat(attributes, hasEntry(equalTo(MSG_ID.key()), not(isEmptyOrNullString())));
        assertThat(attributes, hasEntry(equalTo(MSG_SENDER_TS.key()), not(isEmptyOrNullString())));
        assertThat(attributes, hasEntry(equalTo(MSG_ARRIVAL_TS.key()), not(isEmptyOrNullString())));
        assertThat(attributes, hasEntry(equalTo(MSG_RECEIVER_TS.key()), not(isEmptyOrNullString())));
        assertThat(attributes, hasEntry(MSG_SENDER.key(), "Synapse"));
        assertThat(receivedMessage.getPayload(), is(somePayload("foo")));
        assertThat(receivedMessage.getKey(), is(instanceOf(SimpleKey.class)));

        //
        // As you can see, the arrival TS is in seconds precision, the other two have microseconds, which makes
        // it difficult to compare them with isAfter without truncating them to full seconds
        // "synapse_msg_receiver_ts" -> "2022-01-25T19:04:06.354671Z"
        // "synapse_msg_sender_ts" -> "2022-01-25T19:03:58.129246Z"
        // "synapse_msg_arrival_ts" -> "2022-01-25T19:03:58Z"
        //
        final Instant sent = receivedMessage.getHeader().getAsInstant(MSG_SENDER_TS).truncatedTo(ChronoUnit.SECONDS);
        final Instant arrived = receivedMessage.getHeader().getAsInstant(MSG_ARRIVAL_TS);
        final Instant received = receivedMessage.getHeader().getAsInstant(MSG_RECEIVER_TS).truncatedTo(ChronoUnit.SECONDS);;
        assertThat(sent.isAfter(arrived), is(false));
        assertThat(arrived.isAfter(received), is(false));
    }

    @Test
    public void shouldSendAndReceiveKinesisMessageInV2FormatWithCustomHeaders() {
        final Header header = of(ImmutableMap.of(
                "string", "some value",
                "timestamp", ofEpochSecond(42).toString())
        );
        final Message<SomePayload> receivedMessage = sendMessageAndAwait(message("", header, somePayload("")), kinesisV2Sender);
        final ImmutableMap<String, String> attributes = receivedMessage.getHeader().getAll();
        assertThat(attributes, hasEntry("string", "some value"));
        assertThat(attributes, hasEntry("timestamp", "1970-01-01T00:00:42Z"));
    }

    @Test
    public void shouldIgnoreBrokenMessage() {
        final String messageId = "urn:message:kinesis:" + UUID.randomUUID().toString();
        final Message<String> brokenMessage = message(messageId, "{foo}");
        kinesisSender
                .send(brokenMessage)
                .join();
        final Message<SomePayload> receivedMessage = sendMessageAndAwait(message("",somePayload("ok again")), kinesisV2Sender);
        assertThat(receivedMessage.getPayload().foo, is("ok again"));
    }


    private Message<SomePayload> sendMessageAndAwait(final Message<?> message,
                                                     final MessageSenderEndpoint kinesisSender) {
        final AtomicReference<Message<SomePayload>> result = new AtomicReference<>();
        final String messageId = "urn:message:kinesis:" + UUID.randomUUID().toString();
        kinesisSender
                .send(copyOf(message)
                        .withKey(message.getKey() instanceof SimpleKey ? of(messageId) : of(messageId, message.getKey().compactionKey()))
                        .build())
                .join();
        await()
                .atMost(30, SECONDS)
                .until(() -> {
                    final Message<SomePayload> msg = receivedMessages.get(messageId);
                    if (msg != null && msg.getKey().partitionKey().equals(messageId)) {
                        result.set(copyOf(msg).withKey(message.getKey()).build());
                        return true;
                    }
                    return false;
                });
        return result.get();
    }

    final static class SomePayload {

        @JsonProperty
        public String foo;

        static SomePayload somePayload(final String foo) {
            SomePayload somePayload = new SomePayload();
            somePayload.foo = foo;
            return somePayload;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SomePayload)) return false;
            SomePayload that = (SomePayload) o;
            return Objects.equals(foo, that.foo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(foo);
        }
    }
}
