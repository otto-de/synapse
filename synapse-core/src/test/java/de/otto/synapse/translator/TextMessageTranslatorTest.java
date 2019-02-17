package de.otto.synapse.translator;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import org.junit.Test;

import static de.otto.synapse.message.Message.message;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TextMessageTranslatorTest {

    @Test
    public void shouldTranslateMessage() {
        final MessageTranslator<TextMessage> messageTranslator = new TextMessageTranslator();
        final Message<String> message = messageTranslator.apply(
                message("test", singletonMap("foo", "bar"))
        );
        assertThat(message.getKey(), is(Key.of("test")));
        assertThat(message.getPayload(), is("{\"foo\":\"bar\"}"));
    }

    @Test
    public void shouldKeepHeadersOfMessage() {
        final MessageTranslator<TextMessage> messageTranslator = new TextMessageTranslator();
        final Message<String> message = messageTranslator.apply(
                message("test", Header.of(null, ImmutableMap.of("foo", "bar")), null)
        );
        assertThat(message.getHeader().getAll(), is(singletonMap("foo", "bar")));
    }

    @Test
    public void shouldTranslateDeleteMessage() {
        final MessageTranslator<TextMessage> messageTranslator = new TextMessageTranslator();
        final Message<String> message = messageTranslator.apply(
                message("test", null)
        );
        assertThat(message.getKey(), is(Key.of("test")));
        assertThat(message.getPayload(), is(nullValue()));
    }
}