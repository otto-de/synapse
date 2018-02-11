package de.otto.edison.eventsourcing.translator;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.message.Message;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static de.otto.edison.eventsourcing.message.Message.message;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class JsonByteBufferMessageTranslatorTest {

    private ObjectMapper objectMapper;

    @Test
    public void shouldTranslateMessage() {
        objectMapper = new ObjectMapper();
        JsonByteBufferMessageTranslator messageTranslator = new JsonByteBufferMessageTranslator(objectMapper);
        final Message<ByteBuffer> message = messageTranslator.translate(
                message("test", singletonMap("foo", "bar"))
        );
        assertThat(message.getKey(), is("test"));
        final ByteBuffer payload = message.getPayload();
        final String s = new String(payload.array(), Charset.forName("UTF-8"));
        assertThat(s, is("{\"foo\":\"bar\"}"));
    }

    @Test
    public void shouldTranslateDeleteMessage() {
        objectMapper = new ObjectMapper();
        MessageTranslator<ByteBuffer> messageTranslator = new JsonByteBufferMessageTranslator(objectMapper);
        final Message<ByteBuffer> message = messageTranslator.translate(
                message("test", null)
        );
        assertThat(message.getKey(), is("test"));
        assertThat(message.getPayload(), is(nullValue()));
    }
}