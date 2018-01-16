package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KinesisEventSenderTest {

    @Mock
    private KinesisStream kinesisStream;

    @Mock
    private TextEncryptor textEncryptor;

    private ObjectMapper objectMapper = new ObjectMapper();
    private KinesisEventSender kinesisEventSender;

    @Captor
    private ArgumentCaptor<Map<String, ByteBuffer>> byteBufferMapCaptor;

    @Before
    public void setUp() throws Exception {
        kinesisEventSender = new KinesisEventSender(kinesisStream, objectMapper, textEncryptor);
    }

    @Test
    public void shouldSendEncryptedEvent() throws Exception {
        // given
        String inputValue = "banana";
        String expectedValue = "encryptedBanana";
        given(textEncryptor.encrypt(eq(new ExampleJsonObject(inputValue).toJson())))
                .willReturn(new ExampleJsonObject(expectedValue).toJson());

        // when
        kinesisEventSender.sendEvent("someKey", new ExampleJsonObject(inputValue));

        // then
        ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(kinesisStream).send(eq("someKey"), captor.capture());

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(captor.getValue());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(jsonObject.value, is(expectedValue));
    }

    @Test
    public void shouldSendUnencryptedEvent() throws Exception {
        // given
        final boolean encryptEvent = false;
        given(textEncryptor.encrypt(anyString())).willReturn(new ExampleJsonObject("wrong string").toJson());

        // when
        kinesisEventSender.sendEvent("someKey", new ExampleJsonObject("banana"), encryptEvent);

        // then
        ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(kinesisStream).send(eq("someKey"), captor.capture());

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(captor.getValue());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(jsonObject.value, is("banana"));
    }


    @Test
    public void shouldSendEncryptedEventWhenFlagIsUsed() throws Exception {
        // given
        String inputValue = "banana";
        String expectedValue = "encryptedBanana";
        given(textEncryptor.encrypt(eq(new ExampleJsonObject(inputValue).toJson())))
                .willReturn(new ExampleJsonObject(expectedValue).toJson());

        // when
        kinesisEventSender.sendEvent("someKey", new ExampleJsonObject(inputValue), true);

        // then
        ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(kinesisStream).send(eq("someKey"), captor.capture());

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(captor.getValue());
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(jsonObject.value, is(expectedValue));
    }

    @Test
    public void shouldSendMultipleEncryptedEvents() throws Exception {
        // given
        ExampleJsonObject bananaObject = new ExampleJsonObject("banana");
        ExampleJsonObject appleObject = new ExampleJsonObject("apple");
        String expected = "encrypted";
        given(textEncryptor.encrypt(any())).willReturn(new ExampleJsonObject(expected).toJson());

        // when
        kinesisEventSender.sendEvents(ImmutableMap.of(
                "b", bananaObject,
                "a", appleObject
        ));

        // then
        verify(kinesisStream).sendMultiple(byteBufferMapCaptor.capture());

        Map<String, ByteBuffer> events = byteBufferMapCaptor.getValue();
        assertThat(events.size(), is(2));

        assertThat(events.keySet(), hasItems("a", "b"));

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(events.get("a"));
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(jsonObject.value, is(expected));
    }

    @Test
    public void shouldSendMultipleUnencryptedEvents() throws Exception {
        // given
        boolean encryptEvents = false;
        ExampleJsonObject bananaObject = new ExampleJsonObject("banana");
        ExampleJsonObject appleObject = new ExampleJsonObject("apple");
        String expected = "encrypted";
        given(textEncryptor.encrypt(any())).willReturn(new ExampleJsonObject(expected).toJson());

        // when
        kinesisEventSender.sendEvents(ImmutableMap.of(
                "b", bananaObject,
                "a", appleObject
        ), encryptEvents);

        // then
        verify(kinesisStream).sendMultiple(byteBufferMapCaptor.capture());

        Map<String, ByteBuffer> events = byteBufferMapCaptor.getValue();
        assertThat(events.size(), is(2));

        assertThat(events.keySet(), hasItems("a", "b"));

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(events.get("a"));
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(jsonObject.value, is("apple"));
    }

    @Test
    public void shouldSendMultipleEncryptedEventsWhenFlagIsUsed() throws Exception {
        // given
        boolean encryptEvents = true;
        ExampleJsonObject bananaObject = new ExampleJsonObject("banana");
        ExampleJsonObject appleObject = new ExampleJsonObject("apple");
        String expected = "encrypted";
        given(textEncryptor.encrypt(any())).willReturn(new ExampleJsonObject(expected).toJson());

        // when
        kinesisEventSender.sendEvents(ImmutableMap.of(
                "b", bananaObject,
                "a", appleObject
        ), encryptEvents);

        // then
        verify(kinesisStream).sendMultiple(byteBufferMapCaptor.capture());

        Map<String, ByteBuffer> events = byteBufferMapCaptor.getValue();
        assertThat(events.size(), is(2));

        assertThat(events.keySet(), hasItems("a", "b"));

        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(events.get("a"));
        ExampleJsonObject jsonObject = objectMapper.readValue(inputStream, ExampleJsonObject.class);

        assertThat(jsonObject.value, is(expected));
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