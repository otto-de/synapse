package de.otto.edison.eventsourcing.aws.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import de.otto.edison.eventsourcing.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import static de.otto.edison.eventsourcing.message.Message.message;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageSenderTest {

    @Mock
    private KinesisStream kinesisStream;

    private ObjectMapper objectMapper = new ObjectMapper();
    private KinesisMessageSender kinesisEventSender;

    @Captor
    private ArgumentCaptor<Stream<Message<ByteBuffer>>> byteBufferMapCaptor;

    @Before
    public void setUp() throws Exception {
        kinesisEventSender = new KinesisMessageSender(kinesisStream, objectMapper);
    }

    @Test
    public void shouldSendEvent() throws Exception {
        // when
        kinesisEventSender.send("someKey", new ExampleJsonObject("banana"));

        // then
        ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(kinesisStream).send(eq("someKey"), captor.capture());

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(captor.getValue());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(jsonObject.value, is("banana"));
    }

    @Test
    public void shouldSendMultipleEvents() throws Exception {
        // given
        ExampleJsonObject bananaObject = new ExampleJsonObject("banana");
        ExampleJsonObject appleObject = new ExampleJsonObject("apple");

        // when
        kinesisEventSender.sendBatch(Stream.of(
                message("b", bananaObject),
                message("a", appleObject)
        ));

        // then
        verify(kinesisStream).sendBatch(byteBufferMapCaptor.capture());

        List<Message<ByteBuffer>> events = byteBufferMapCaptor.getValue().collect(toList());
        assertThat(events.size(), is(2));

        assertThat(events.stream().map(Message::getKey).collect(toList()), contains("b", "a"));

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(events.get(0).getPayload());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(jsonObject.value, is("banana"));
    }

    @Test
    public void shouldSendDeleteEventWithEmptyByteBuffer() throws JsonProcessingException {
        //when
        kinesisEventSender.send("someKey", null);

        //then
        verify(kinesisStream).send("someKey", ByteBuffer.allocateDirect(0));
    }

    @Test
    public void shouldSendDeleteEventWithEmptyByteBufferWithoutEncryption() throws JsonProcessingException {
        //when
        kinesisEventSender.send("someKey", null);

        //then
        verify(kinesisStream).send("someKey", ByteBuffer.allocateDirect(0));
    }

    private static class ExampleJsonObject {
        @JsonProperty
        private String value;

        public ExampleJsonObject() {
        }

        public ExampleJsonObject(String value) {
            this.value = value;
        }

        public String toJson() {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
