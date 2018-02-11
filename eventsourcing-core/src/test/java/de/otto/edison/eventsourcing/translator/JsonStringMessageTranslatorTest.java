package de.otto.edison.eventsourcing.translator;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.message.Message;
import org.junit.Test;

import static de.otto.edison.eventsourcing.message.Message.message;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class JsonStringMessageTranslatorTest {

    private ObjectMapper objectMapper;

    @Test
    public void shouldTranslateMessage() {
        objectMapper = new ObjectMapper();
        MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator(objectMapper);
        final Message<String> message = messageTranslator.translate(
                message("test", singletonMap("foo", "bar"))
        );
        assertThat(message.getKey(), is("test"));
        assertThat(message.getPayload(), is("{\"foo\":\"bar\"}"));
    }

    @Test
    public void shouldTranslateDeleteMessage() {
        objectMapper = new ObjectMapper();
        MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator(objectMapper);
        final Message<String> message = messageTranslator.translate(
                message("test", null)
        );
        assertThat(message.getKey(), is("test"));
        assertThat(message.getPayload(), is(nullValue()));
    }
}