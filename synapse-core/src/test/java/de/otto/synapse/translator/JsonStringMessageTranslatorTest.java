package de.otto.synapse.translator;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import org.junit.Test;

import java.time.Instant;

import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class JsonStringMessageTranslatorTest {

    private static final Instant NOW = Instant.now();

    @Test
    public void shouldTranslateMessage() {
        final MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator();
        final Message<String> message = messageTranslator.translate(
                message("test", singletonMap("foo", "bar"))
        );
        assertThat(message.getKey(), is(Key.of("test")));
        assertThat(message.getPayload(), is("{\"foo\":\"bar\"}"));
    }

    @Test
    public void shouldKeepHeadersOfMessage() {
        final MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator();
        final Message<String> message = messageTranslator.translate(
                message("test", responseHeader(null, ImmutableMap.of("foo", "bar")), null)
        );
        assertThat(message.getHeader().getAll(), is(singletonMap("foo", "bar")));
    }

    @Test
    public void shouldTranslateDeleteMessage() {
        final MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator();
        final Message<String> message = messageTranslator.translate(
                message("test", null)
        );
        assertThat(message.getKey(), is(Key.of("test")));
        assertThat(message.getPayload(), is(nullValue()));
    }
}