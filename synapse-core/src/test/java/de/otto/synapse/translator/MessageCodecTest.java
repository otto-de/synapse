package de.otto.synapse.translator;

import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import org.junit.Test;

import static com.google.common.collect.ImmutableBiMap.of;
import static de.otto.synapse.message.Key.NO_KEY;
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
        final Message<String> someMessage = Message.message(Key.of("foo", "bar"), Header.builder().withAttributes(of("attr", "value")).build(), "{}");
        final String encoded = encode(someMessage, MessageFormat.V2);
        assertThat(encoded).isEqualTo("{\"_synapse_msg_format\":\"v2\",\"_synapse_msg_key\":{\"partitionKey\":\"foo\",\"compactionKey\":\"bar\"},\"_synapse_msg_headers\":{\"attr\":\"value\"},\"_synapse_msg_payload\":{}}");
    }

    @Test
    public void shouldDecodeV1FormatIntoMessage() {
        final String body = "{\"foo\":\"bar\"}";
        final Message<String> message = decode(body, Header.builder(), Message.builder(String.class));

        assertThat(message.getKey()).isEqualTo(NO_KEY);
        assertThat(message.getPayload()).isEqualTo(body);
        assertThat(message.getHeader().getAll()).isEmpty();
    }

    @Test
    public void shouldDecodeNonJsonV1FormatIntoMessage() {
        final String body = "some non-json body";
        final Message<String> message = decode(body, Header.builder(), Message.builder(String.class));

        assertThat(message.getKey()).isEqualTo(NO_KEY);
        assertThat(message.getPayload()).isEqualTo(body);
        assertThat(message.getHeader().getAll()).isEmpty();
    }

    @Test
    public void shouldDecodeV1FormatWithNullPayloadIntoMessage() {
        final String body = null;
        final Message<String> message = decode(body, Header.builder(), Message.builder(String.class));

        assertThat(message.getKey()).isEqualTo(NO_KEY);
        assertThat(message.getPayload()).isNull();
        assertThat(message.getHeader().getAll()).isEmpty();
    }

    @Test
    public void shouldDecodeV2FormatIntoMessage() {
        final String body = "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_headers\":{\"attr\":\"value\"},"
                + "\"_synapse_msg_payload\":{\"some\":\"payload\"}}";

        final Message<String> message = decode(body, Header.builder(), Message.builder(String.class));

        assertThat(message.getKey()).isEqualTo(Key.of());
        assertThat(message.getPayload()).isEqualTo("{\"some\":\"payload\"}");
        assertThat(message.getHeader().getAsString("attr")).isEqualTo("value");
    }

    @Test
    public void shouldDecodeV2FormatWithCompoundKeyIntoMessage() {
        final String body = "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_key\":{\"partitionKey\":\"p1\",\"compactionKey\":\"p2\"},"
                + "\"_synapse_msg_headers\":{\"attr\":\"value\"},"
                + "\"_synapse_msg_payload\":{\"some\":\"payload\"}}";

        final Message<String> message = decode(body, Header.builder(), Message.builder(String.class));

        assertThat(message.getKey()).isEqualTo(Key.of("p1", "p2"));
        assertThat(message.getPayload()).isEqualTo("{\"some\":\"payload\"}");
        assertThat(message.getHeader().getAsString("attr")).isEqualTo("value");
    }

    @Test
    public void shouldDecodeV2FormatWithNullPayloadIntoMessage() {
        final String body = "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_headers\":{},"
                + "\"_synapse_msg_payload\":null}";

        final Message<String> message = decode(body, Header.builder(), Message.builder(String.class));

        assertThat(message.getPayload()).isNull();
        assertThat(message.getHeader().getAll()).isEmpty();
    }

}