package de.otto.synapse.endpoint.sender.aws;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.allChannelsWith;
import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageSenderTest {

    private KinesisMessageSender kinesisMessageSender;

    @Mock
    private KinesisAsyncClient kinesisClient;
    @Captor
    private ArgumentCaptor<PutRecordsRequest> putRecordsRequestCaptor;
    private ObjectMapper objectMapper = new ObjectMapper();
    private MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator(objectMapper);

    @Before
    public void setUp() {
        kinesisMessageSender = new KinesisMessageSender("test", messageTranslator, kinesisClient);
    }

    @Test
    public void shouldSendEvent() throws Exception {
        // given
        final Message<ExampleJsonObject> message = message("someKey", new ExampleJsonObject("banana"));

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));

        // when
        kinesisMessageSender.send(message);

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        assertThat(caputuredRequest.streamName(), is("test"));
        assertThat(caputuredRequest.records(), hasSize(1));
        assertThat(caputuredRequest.records().get(0).partitionKey(), is("someKey"));

        final ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(caputuredRequest.records().get(0).data().asByteBuffer());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);
        assertThat(jsonObject.value, is("banana"));

    }

    @Test
    public void shouldInterceptMessages() throws IOException {
        // given
        final Message<ExampleJsonObject> message = message("someKey", new ExampleJsonObject("banana"));

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));
        // and especially
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        registry.register(allChannelsWith((m) -> message(m.getKey(), m.getHeader(), "{\"value\" : \"apple\"}")));
        kinesisMessageSender.registerInterceptorsFrom(registry);

        // when
        kinesisMessageSender.send(message);

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        final ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(caputuredRequest.records().get(0).data().asByteBuffer());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);
        assertThat(jsonObject.value, is("apple"));
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

        // and especially
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        registry.register(allChannelsWith((m) -> message(m.getKey(), m.getHeader(), "{\"value\" : \"Lovely day for a Guinness\"}")));
        kinesisMessageSender.registerInterceptorsFrom(registry);

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

        assertThat(objectMapper.readValue(
                new ByteBufferBackedInputStream(firstEntry.data().asByteBuffer()), ExampleJsonObject.class).value,
                is("Lovely day for a Guinness"));

        final PutRecordsRequestEntry secondEntry = caputuredRequest.records().get(1);
        assertThat(secondEntry.partitionKey(), is("a"));

        assertThat(objectMapper.readValue(
                new ByteBufferBackedInputStream(secondEntry.data().asByteBuffer()), ExampleJsonObject.class).value,
                is("Lovely day for a Guinness"));
    }

    @Test
    public void shouldInterceptMessagesInBatch() throws Exception {
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

        assertThat(objectMapper.readValue(
                new ByteBufferBackedInputStream(firstEntry.data().asByteBuffer()), ExampleJsonObject.class).value,
                is("banana"));

        final PutRecordsRequestEntry secondEntry = caputuredRequest.records().get(1);
        assertThat(secondEntry.partitionKey(), is("a"));

        assertThat(objectMapper.readValue(
                new ByteBufferBackedInputStream(secondEntry.data().asByteBuffer()), ExampleJsonObject.class).value,
                is("apple"));
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
    public void shouldSendDeleteEventWithEmptyByteBuffer() {
        // given
        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(completedFuture(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(PutRecordsResultEntry.builder().build())
                .build()));

        //when
        kinesisMessageSender.send(message("someKey", null));

        //then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        assertThat(putRecordsRequestCaptor.getValue().records().get(0).partitionKey(), is("someKey"));
        assertThat(putRecordsRequestCaptor.getValue().records().get(0).data(), is(SdkBytes.fromByteBuffer(ByteBuffer.allocateDirect(0))));
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
