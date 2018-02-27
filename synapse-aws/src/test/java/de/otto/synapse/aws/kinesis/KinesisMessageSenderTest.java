package de.otto.synapse.aws.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.JsonByteBufferMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageSenderTest {

    private KinesisMessageSender kinesisMessageSender;

    @Mock
    private KinesisClient kinesisClient;
    @Captor
    private ArgumentCaptor<PutRecordsRequest> putRecordsRequestCaptor;
    private ObjectMapper objectMapper = new ObjectMapper();
    private MessageTranslator<ByteBuffer> messageTranslator = new JsonByteBufferMessageTranslator(objectMapper);

    @Before
    public void setUp() throws Exception {
        kinesisMessageSender = new KinesisMessageSender("test", messageTranslator, kinesisClient);
    }

    @Test
    public void shouldSendEvent() throws Exception {
        // given
        final Message<ExampleJsonObject> message = message("someKey", new ExampleJsonObject("banana"));

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .build());

        // when
        kinesisMessageSender.send(message);

        // then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        final PutRecordsRequest caputuredRequest = putRecordsRequestCaptor.getValue();

        assertThat(caputuredRequest.streamName(), is("test"));
        assertThat(caputuredRequest.records(), hasSize(1));
        assertThat(caputuredRequest.records().get(0).partitionKey(), is("someKey"));

        final ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(caputuredRequest.records().get(0).data());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);
        assertThat(jsonObject.value, is("banana"));

    }

    @Test
    public void shouldSendMultipleEvents() throws Exception {
        // given
        ExampleJsonObject bananaObject = new ExampleJsonObject("banana");
        ExampleJsonObject appleObject = new ExampleJsonObject("apple");

        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .build());

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
                new ByteBufferBackedInputStream(firstEntry.data()), ExampleJsonObject.class).value,
                is("banana"));

        final PutRecordsRequestEntry secondEntry = caputuredRequest.records().get(1);
        assertThat(secondEntry.partitionKey(), is("a"));

        assertThat(objectMapper.readValue(
                new ByteBufferBackedInputStream(secondEntry.data()), ExampleJsonObject.class).value,
                is("apple"));

    }

    @Test
    public void shouldBatchEventsWhenTooManyShouldBeSent() throws Exception {
        // given
        PutRecordsResponse putRecordsResponse = PutRecordsResponse.builder()
                .failedRecordCount(0)
                .build();
        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(putRecordsResponse);

        // when
        kinesisMessageSender.sendBatch(someEvents(500 + 1));

        // then
        verify(kinesisClient, times(2)).putRecords(any(PutRecordsRequest.class));
    }

    @Test
    public void shouldSendDeleteEventWithEmptyByteBuffer() throws JsonProcessingException {
        // given
        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(PutRecordsResponse.builder()
                .failedRecordCount(0)
                .build());

        //when
        kinesisMessageSender.send("someKey", null);

        //then
        verify(kinesisClient).putRecords(putRecordsRequestCaptor.capture());
        assertThat(putRecordsRequestCaptor.getValue().records().get(0).partitionKey(), is("someKey"));
        assertThat(putRecordsRequestCaptor.getValue().records().get(0).data(), is(ByteBuffer.allocateDirect(0)));
    }

    private Stream<Message<ByteBuffer>> someEvents(int n) {
        return IntStream.range(0, n)
                .mapToObj(i -> message(valueOf(i), ByteBuffer.wrap(Integer.toString(i).getBytes(StandardCharsets.UTF_8))));
    }

    private static class ExampleJsonObject {
        @JsonProperty
        private String value;

        public ExampleJsonObject() {
        }

        public ExampleJsonObject(String value) {
            this.value = value;
        }

    }
}
