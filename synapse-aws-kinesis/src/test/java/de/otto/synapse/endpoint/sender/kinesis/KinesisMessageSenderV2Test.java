package de.otto.synapse.endpoint.sender.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.kinesis.KinesisMessage;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.of;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.matchingSenderChannelsWith;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.senderChannelsWith;
import static de.otto.synapse.message.Header.requestHeader;
import static de.otto.synapse.message.Message.message;
import static de.otto.synapse.message.kinesis.KinesisMessage.*;
import static de.otto.synapse.translator.ObjectMappers.defaultObjectMapper;
import static java.lang.String.valueOf;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageSenderV2Test {

    private KinesisMessageSender kinesisMessageSender;

    @Mock
    private KinesisAsyncClient kinesisClient;
    @Captor
    private ArgumentCaptor<PutRecordsRequest> putRecordsRequestCaptor;
    private MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator();
    private MessageInterceptorRegistry interceptorRegistry;

    @Before
    public void setUp() {
        interceptorRegistry = new MessageInterceptorRegistry();
        kinesisMessageSender = new KinesisMessageSender("test", interceptorRegistry, messageTranslator, kinesisClient, KinesisMessage.Format.V2);
    }

    @Test
    public void shouldSendMessage() throws Exception {
        // given
        final Message<ExampleJsonObject> message = message("someKey", new ExampleJsonObject("banana"));

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));

        // when
        kinesisMessageSender.send(message).join();

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        assertThat(caputuredRequest.streamName(), is("test"));
        assertThat(caputuredRequest.records(), hasSize(1));
        assertThat(caputuredRequest.records().get(0).partitionKey(), is("someKey"));

        final ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(caputuredRequest.records().get(0).data().asByteBuffer());

        final JsonNode json = defaultObjectMapper().readTree(inputStream);
        assertThat(json.get(SYNAPSE_MSG_FORMAT).asText(), is("v2"));
    }

    @Test
    public void shouldSendMessageHeaders() throws Exception {
        // given
        final Message<ExampleJsonObject> message = message("someKey", requestHeader(of("attr-key", "attr-value")), null);

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));

        // when
        kinesisMessageSender.send(message).join();

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        assertThat(caputuredRequest.streamName(), is("test"));
        assertThat(caputuredRequest.records(), hasSize(1));
        assertThat(caputuredRequest.records().get(0).partitionKey(), is("someKey"));

        final ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(caputuredRequest.records().get(0).data().asByteBuffer());

        final JsonNode json = defaultObjectMapper().readTree(inputStream);

        assertThat(defaultObjectMapper().convertValue(json.get(SYNAPSE_MSG_HEADERS), Map.class), is(ImmutableMap.of("attr-key", "attr-value")));
    }

    @Test
    public void shouldSendMessagePayload() throws Exception {
        // given
        final Message<ExampleJsonObject> message = message("someKey", new ExampleJsonObject("banana"));

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));

        // when
        kinesisMessageSender.send(message).join();

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        assertThat(caputuredRequest.streamName(), is("test"));
        assertThat(caputuredRequest.records(), hasSize(1));
        assertThat(caputuredRequest.records().get(0).partitionKey(), is("someKey"));

        final ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(caputuredRequest.records().get(0).data().asByteBuffer());

        final JsonNode json = defaultObjectMapper().readTree(inputStream);
        final ExampleJsonObject jsonObject = defaultObjectMapper().convertValue(json.get(SYNAPSE_MSG_PAYLOAD), ExampleJsonObject.class);
        assertThat(jsonObject.value, is("banana"));
    }

    @Test
    public void shouldSendMessageWithNonJsonPayload() {
        // given
        final Message<String> message = message("someKey", "some non-json payload");

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));

        // when
        kinesisMessageSender.send(message).join();

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        assertThat(caputuredRequest.streamName(), is("test"));
        assertThat(caputuredRequest.records(), hasSize(1));
        assertThat(caputuredRequest.records().get(0).partitionKey(), is("someKey"));

        final String payload = caputuredRequest.records().get(0).data().asString(Charset.forName("UTF-8"));

        assertThat(payload, is("{\"_synapse_msg_format\":\"v2\",\"_synapse_msg_headers\":{},\"_synapse_msg_payload\":\"some non-json payload\"}"));
    }

    @Test
    public void shouldSendMessageWithBrokenJsonPayload() {
        // given
        final Message<String> message = message("someKey", "{\"some\": broken json payload}");

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));

        // when
        kinesisMessageSender.send(message).join();

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        assertThat(caputuredRequest.streamName(), is("test"));
        assertThat(caputuredRequest.records(), hasSize(1));
        assertThat(caputuredRequest.records().get(0).partitionKey(), is("someKey"));

        final String payload = caputuredRequest.records().get(0).data().asString(Charset.forName("UTF-8"));

        assertThat(payload, is("{\"_synapse_msg_format\":\"v2\",\"_synapse_msg_headers\":{},\"_synapse_msg_payload\":{\"some\": broken json payload}}"));
    }

    @Test
    public void shouldInterceptMessages() throws IOException {
        // given
        final Message<String> message = message("someKey", null);

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build())
        );
        // and especially
        interceptorRegistry.register(matchingSenderChannelsWith(
                "test",
                (m) -> message(m.getKey(), m.getHeader(), "{\"value\":\"apple\"}"))
        );

        // when
        kinesisMessageSender.send(message).join();

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        final String recordBody = caputuredRequest.records().get(0).data().asUtf8String();
        final String payload = defaultObjectMapper().readTree(recordBody).get(SYNAPSE_MSG_PAYLOAD).toString();
        assertThat(payload, is("{\"value\":\"apple\"}"));
    }


    @Test
    public void shouldSendBatch() throws Exception {
        // given
        ExampleJsonObject bananaObject = new ExampleJsonObject("banana");
        ExampleJsonObject appleObject = new ExampleJsonObject("apple");

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));

        // when
        kinesisMessageSender.sendBatch(Stream.of(
                message("b", bananaObject),
                message("a", appleObject)
        ));

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        assertThat(caputuredRequest.streamName(), is("test"));
        assertThat(caputuredRequest.records(), hasSize(2));

        final PutRecordsRequestEntry firstEntry = caputuredRequest.records().get(0);
        assertThat(firstEntry.partitionKey(), is("b"));

        assertThat(defaultObjectMapper().readValue(
                new ByteBufferBackedInputStream(firstEntry.data().asByteBuffer()), Map.class).get(SYNAPSE_MSG_PAYLOAD),
                is(singletonMap("value", "banana")));

        final PutRecordsRequestEntry secondEntry = caputuredRequest.records().get(1);
        assertThat(secondEntry.partitionKey(), is("a"));

        assertThat(defaultObjectMapper().readValue(
                new ByteBufferBackedInputStream(secondEntry.data().asByteBuffer()), Map.class).get(SYNAPSE_MSG_PAYLOAD),
                is(singletonMap("value", "apple")));
    }

    @Test
    public void shouldInterceptMessagesInBatch() throws Exception {
        // given
        ExampleJsonObject first = new ExampleJsonObject("x");
        ExampleJsonObject second = new ExampleJsonObject("x");

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));

        interceptorRegistry.register(senderChannelsWith(m -> message(m.getKey(), "{\"m\":\"Lovely day for a Guinness\"}")));

        // when
        kinesisMessageSender.sendBatch(Stream.of(
                message("b", first),
                message("a", second)
        ));

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        assertThat(caputuredRequest.streamName(), is("test"));
        assertThat(caputuredRequest.records(), hasSize(2));

        final PutRecordsRequestEntry firstEntry = caputuredRequest.records().get(0);
        assertThat(firstEntry.partitionKey(), is("b"));

        assertThat(defaultObjectMapper().readValue(
                new ByteBufferBackedInputStream(firstEntry.data().asByteBuffer()), Map.class).get(SYNAPSE_MSG_PAYLOAD),
                is(singletonMap("m", "Lovely day for a Guinness")));

        final PutRecordsRequestEntry secondEntry = caputuredRequest.records().get(1);
        assertThat(secondEntry.partitionKey(), is("a"));

        assertThat(defaultObjectMapper().readValue(
                new ByteBufferBackedInputStream(secondEntry.data().asByteBuffer()), Map.class).get(SYNAPSE_MSG_PAYLOAD),
                is(singletonMap("m", "Lovely day for a Guinness")));
    }

    @Test
    public void shouldBatchEventsWhenTooManyShouldBeSent() {
        // given
        PutRecordsResponse putRecordsResponse = PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build();
        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(putRecordsResponse));

        // when
        kinesisMessageSender.sendBatch(someEvents(500 + 1));

        // then
        verify(kinesisClient, times(2)).putRecords(any(PutRecordsRequest.class));
    }

    @Test
    public void shouldSendDeleteMessage() throws IOException {
        // given
        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));

        //when
        kinesisMessageSender.send(message("someKey", null)).join();

        //then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        final ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(caputuredRequest.records().get(0).data().asByteBuffer());

        final JsonNode json = defaultObjectMapper().readTree(inputStream);
        assertThat(json.get(SYNAPSE_MSG_PAYLOAD).textValue(), is(nullValue()));
    }

    private Stream<Message<String>> someEvents(int n) {
        return IntStream.range(0, n)
                .mapToObj(i -> message(valueOf(i), Integer.toString(i)));
    }

    private static class ExampleJsonObject {
        @JsonProperty
        private String value;

        public ExampleJsonObject() {
        }

        ExampleJsonObject(String value) {
            this.value = value;
        }

    }
}
