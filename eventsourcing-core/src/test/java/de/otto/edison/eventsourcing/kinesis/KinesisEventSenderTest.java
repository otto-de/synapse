package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KinesisEventSenderTest {

    @Mock
    private KinesisStream kinesisStream;

    private TextEncryptor textEncryptor = Encryptors.noOpText();

    private ObjectMapper objectMapper = new ObjectMapper();


    @Test
    public void shouldSendEvent() throws Exception {
        //given
        KinesisEventSender kinesisEventSender = new KinesisEventSender(kinesisStream, objectMapper, textEncryptor);

        // when
        kinesisEventSender.sendEvent("someKey", new ExampleJsonObject("banana"));

        // then
        ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(kinesisStream).send(eq("someKey"), captor.capture());

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(captor.getValue());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(jsonObject.value, is("banana"));
    }

    static class ExampleJsonObject {
        @JsonProperty
        private String value;

        public ExampleJsonObject() {
        }

        public ExampleJsonObject(String value) {
            this.value = value;
        }
    }
}