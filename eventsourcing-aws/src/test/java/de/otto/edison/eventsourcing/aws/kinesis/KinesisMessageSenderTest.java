package de.otto.edison.eventsourcing.aws.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import de.otto.edison.eventsourcing.message.Message;
import de.otto.edison.eventsourcing.translator.JsonByteBufferMessageTranslator;
import de.otto.edison.eventsourcing.translator.MessageTranslator;
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

import static de.otto.edison.eventsourcing.message.ByteBufferMessage.byteBufferMessage;
import static de.otto.edison.eventsourcing.message.Message.message;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageSenderTest {

    @Mock
    private KinesisStream kinesisStream;

    private KinesisMessageSender kinesisEventSender;

    @Captor
    private ArgumentCaptor<Stream<Message<ByteBuffer>>> byteBufferMessageStreamCaptor;
    @Captor
    private ArgumentCaptor<Message<ByteBuffer>> byteBufferMessageCaptor;
    private ObjectMapper objectMapper = new ObjectMapper();
    private MessageTranslator<ByteBuffer> messageTranslator = new JsonByteBufferMessageTranslator(objectMapper);

    @Before
    public void setUp() throws Exception {
        kinesisEventSender = new KinesisMessageSender(kinesisStream, messageTranslator);
    }

    @Test
    public void shouldSendEvent() throws Exception {
        // given
        final Message<ExampleJsonObject> message = message("someKey", new ExampleJsonObject("banana"));

        // when
        kinesisEventSender.send(message);

        // then
        verify(kinesisStream).send(byteBufferMessageCaptor.capture());

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(byteBufferMessageCaptor.getValue().getPayload());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(byteBufferMessageCaptor.getValue().getKey(), is("someKey"));
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
        verify(kinesisStream).sendBatch(byteBufferMessageStreamCaptor.capture());

        List<Message<ByteBuffer>> events = byteBufferMessageStreamCaptor.getValue().collect(toList());
        assertThat(events.size(), is(2));

        assertThat(events.stream().map(Message::getKey).collect(toList()), contains("b", "a"));

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(events.get(0).getPayload());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(jsonObject.value, is("banana"));
    }

    @Test
    public void shouldSendDeleteEventWithNullByteBuffer() throws JsonProcessingException {
        //when
        kinesisEventSender.send("someKey", null);

        //then
        verify(kinesisStream).send(byteBufferMessage("someKey", (ByteBuffer) null));
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
