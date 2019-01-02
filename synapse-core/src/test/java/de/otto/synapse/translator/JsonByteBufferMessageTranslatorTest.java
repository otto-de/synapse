package de.otto.synapse.translator;

import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Instant;

import static com.google.common.collect.ImmutableMap.of;
import static de.otto.synapse.message.DefaultHeaderAttr.MSG_SENDER_TS;
import static de.otto.synapse.message.Message.message;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class JsonByteBufferMessageTranslatorTest {

    private static final Instant NOW = Instant.now();

    @Test
    public void shouldTranslateMessage() {
        final MessageTranslator<ByteBuffer> messageTranslator = new JsonByteBufferMessageTranslator();
        final Message<ByteBuffer> message = messageTranslator.translate(
                message(Key.of("test"), singletonMap("foo", "bar"))
        );
        assertThat(message.getKey(), is(Key.of("test")));
        final ByteBuffer payload = message.getPayload();
        final String s = new String(payload.array(), Charset.forName("UTF-8"));
        assertThat(s, is("{\"foo\":\"bar\"}"));
    }

    @Test
    public void shouldKeepHeadersOfMessage() {
        final MessageTranslator<ByteBuffer> messageTranslator = new JsonByteBufferMessageTranslator();
        final Message<ByteBuffer> message = messageTranslator.translate(
                message(Key.of("test"), Header.responseHeader(null, of(MSG_SENDER_TS.key(), NOW.toString())), null)
        );
        assertThat(message.getHeader().getAsInstant(MSG_SENDER_TS), is(NOW));
    }

    @Test
    public void shouldTranslateDeleteMessage() {
        final MessageTranslator<ByteBuffer> messageTranslator = new JsonByteBufferMessageTranslator();
        final Message<ByteBuffer> message = messageTranslator.translate(
                message(Key.of("test"), null)
        );
        assertThat(message.getKey(), is(Key.of("test")));
        assertThat(message.getPayload(), is(nullValue()));
    }
}