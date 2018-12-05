package de.otto.synapse.translator;

import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;
import org.junit.Test;

import static com.google.common.collect.ImmutableBiMap.of;
import static de.otto.synapse.translator.MessageCodec.decode;
import static de.otto.synapse.translator.MessageCodec.encode;
import static org.assertj.core.api.Assertions.assertThat;

public class MessageCodecTest {

    @Test
    public void shouldEncodeInDefaultFormat() {
        final String encoded = encode(Message.message("foo", Header.builder().withAttributes(of("attr", "value")).build(), "{}"));
        assertThat(encoded).isEqualTo("{}");
    }

    @Test
    public void shouldEncodeInV1Format() {
        final Message<String> someMessage = Message.message("foo", Header.builder().withAttributes(of("attr", "value")).build(), "{}");
        final String encoded = encode(someMessage, MessageFormat.V1);
        assertThat(encoded).isEqualTo("{}");
    }

    @Test
    public void shouldEncodeInV2Format() {
        final Message<String> someMessage = Message.message("foo", Header.builder().withAttributes(of("attr", "value")).build(), "{}");
        final String encoded = encode(someMessage, MessageFormat.V2);
        assertThat(encoded).isEqualTo("{\"_synapse_msg_format\":\"v2\",\"_synapse_msg_headers\":{\"attr\":\"value\"},\"_synapse_msg_payload\":{}}");
    }

    @Test
    public void shouldDecodeV1FormatIntoMessage() {
        final String body = "{\"foo\":\"bar\"}";
        final Message<String> message = decode(body, Header.builder(), Message.builder(String.class));

        assertThat(message.getPayload()).isEqualTo(body);
        assertThat(message.getHeader().getAttributes()).isEmpty();
    }

    @Test
    public void shouldDecodeV1FormatWithNullPayloadIntoMessage() {
        final String body = null;
        final Message<String> message = decode(body, Header.builder(), Message.builder(String.class));

        assertThat(message.getPayload()).isNull();
        assertThat(message.getHeader().getAttributes()).isEmpty();
    }

    @Test
    public void shouldDecodeV2FormatIntoMessage() {
        final String body = "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_headers\":{\"attr\":\"value\"},"
                + "\"_synapse_msg_payload\":{\"some\":\"payload\"}}";

        final Message<String> message = decode(body, Header.builder(), Message.builder(String.class));

        assertThat(message.getPayload()).isEqualTo("{\"some\":\"payload\"}");
        assertThat(message.getHeader().getStringAttribute("attr")).isEqualTo("value");
    }

    @Test
    public void shouldDecodeV2FormatWithNullPayloadIntoMessage() {
        final String body = "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_headers\":{},"
                + "\"_synapse_msg_payload\":null}";

        final Message<String> message = decode(body, Header.builder(), Message.builder(String.class));

        assertThat(message.getPayload()).isNull();
        assertThat(message.getHeader().getAttributes()).isEmpty();
    }

}