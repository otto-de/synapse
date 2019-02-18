package de.otto.synapse.translator;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import org.junit.Assert;
import org.junit.Test;

import static com.google.common.collect.ImmutableBiMap.of;
import static de.otto.synapse.message.Key.NO_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

public class AbstractTextDecoderTest {

    private final AbstractTextDecoder<String> decoder = new AbstractTextDecoder<String>() {
        @Override
        public TextMessage apply(final String message) {
            return decode(NO_KEY, Header.of(), message);
        }
    };

    @Test
    public void shouldMergePrototypeKeyAndHeadersInV1Format() {
        final Key key = Key.of("foo", "bar");
        final Header header = Header.of(ShardPosition.fromPosition("shard", "42"), ImmutableMap.of("attr", "value"));
        final String payload = "some payload";

        final TextMessage message = decoder.decode(key, header, payload);

        assertThat(message.getKey()).isEqualTo(key);
        assertThat(message.getHeader()).isEqualTo(header);
        assertThat(message.getPayload()).isEqualTo(payload);
    }

    @Test
    public void shouldMergePrototypeKeyAndHeadersInV2Format() {
        final String body = "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_key\":{\"partitionKey\":\"p1\",\"compactionKey\":\"p2\"},"
                + "\"_synapse_msg_headers\":{\"attr\":\"value\"},"
                + "\"_synapse_msg_payload\":{\"some\":\"payload\"}}";


        final Key key = Key.of("foo", "bar");
        final Header header = Header.of(ShardPosition.fromPosition("shard", "42"), ImmutableMap.of("foo", "bar"));

        final TextMessage message = decoder.decode(key, header, body);

        assertThat(message.getKey()).isEqualTo(Key.of("p1", "p2"));
        assertThat(message.getHeader()).isEqualTo(Header.copyOf(header).withAttribute("attr", "value").build());
        assertThat(message.getPayload()).isEqualTo("{\"some\":\"payload\"}");
    }

    @Test
    public void shouldDecodeInV2FormatWithEscapedCharacters() {
        final TextEncoder encoder = new TextEncoder(MessageFormat.V2);
        final TextMessage someMessage = TextMessage.of(Key.of("f\\_oo", "bar"), Header.builder().withAttributes(of("attr", "valu\\e")).build(), "{}");
        final String encoded = encoder.apply(someMessage);
        Message<String> decoded = decoder.apply(encoded);
        assertThat(decoded).isEqualTo(someMessage);
    }

    @Test
    public void shouldDecodeV1FormatIntoMessage() {
        final String body = "{\"foo\":\"bar\"}";
        final Message<String> message = decoder.apply(body);

        assertThat(message.getKey()).isEqualTo(NO_KEY);
        assertThat(message.getPayload()).isEqualTo(body);
        assertThat(message.getHeader().getAll()).isEmpty();
    }

    @Test
    public void shouldDecodeNonJsonV1FormatIntoMessage() {
        final String body = "some non-json body";
        final Message<String> message = decoder.apply(body);

        assertThat(message.getKey()).isEqualTo(NO_KEY);
        assertThat(message.getPayload()).isEqualTo(body);
        assertThat(message.getHeader().getAll()).isEmpty();
    }

    @Test
    public void shouldDecodeV1FormatWithNullPayloadIntoMessage() {
        final Message<String> message = decoder.apply(null);

        assertThat(message.getKey()).isEqualTo(NO_KEY);
        assertThat(message.getPayload()).isNull();
        assertThat(message.getHeader().getAll()).isEmpty();
    }

    @Test
    public void shouldDecodeV2FormatIntoMessage() {
        final String body = "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_headers\":{\"attr\":\"value\"},"
                + "\"_synapse_msg_payload\":{\"some\":\"payload\"}}";

        final Message<String> message = decoder.apply(body);

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

        final Message<String> message = decoder.apply(body);

        assertThat(message.getKey()).isEqualTo(Key.of("p1", "p2"));
        assertThat(message.getPayload()).isEqualTo("{\"some\":\"payload\"}");
        assertThat(message.getHeader().getAsString("attr")).isEqualTo("value");
    }

    @Test
    public void shouldDecodeV2FormatWithNullPayloadIntoMessage() {
        final String body = "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_headers\":{},"
                + "\"_synapse_msg_payload\":null}";

        final Message<String> message = decoder.apply(body);

        assertThat(message.getPayload()).isNull();
        assertThat(message.getHeader().getAll()).isEmpty();
    }

    @Test
    public void shouldConvertMessageToStringValueAndViceVersa() {
        final TextMessage message = TextMessage.of("some key","{}");
        final String value = new TextEncoder(MessageFormat.V2).apply(message);
        Assert.assertThat(value, is("{\"_synapse_msg_format\":\"v2\",\"_synapse_msg_key\":{\"partitionKey\":\"some key\",\"compactionKey\":\"some key\"},\"_synapse_msg_headers\":{},\"_synapse_msg_payload\":{}}"));
        final Message<String> transformed = decoder.apply(value);
        Assert.assertThat(transformed.getKey(), is(message.getKey()));
        Assert.assertThat(transformed.getPayload(), is(message.getPayload()));
    }

    @Test
    public void shouldConvertMessageWithNullPayloadToStringValueAndViceVersa() {
        final TextMessage message = TextMessage.of("some key", null);
        final String value = new TextEncoder(MessageFormat.V2).apply(message);
        Assert.assertThat(value, is("{\"_synapse_msg_format\":\"v2\",\"_synapse_msg_key\":{\"partitionKey\":\"some key\",\"compactionKey\":\"some key\"},\"_synapse_msg_headers\":{},\"_synapse_msg_payload\":null}"));
        final Message<String> transformed = decoder.apply(value);
        Assert.assertThat(transformed.getKey(), is(message.getKey()));
        Assert.assertThat(transformed.getPayload(), is(message.getPayload()));
    }
}