package de.otto.synapse.translator;

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

public class TextEncoderTest {

    @Test
    public void shouldEncodeInDefaultFormat() {
        final TextEncoder encoder = new TextEncoder();
        final String encoded = encoder.apply(TextMessage.of("foo", Header.builder().withAttributes(of("attr", "value")).build(), "{}"));
        assertThat(encoded).isEqualTo("{}");
    }

    @Test
    public void shouldEncodeInV1Format() {
        final TextEncoder encoder = new TextEncoder(MessageFormat.V1);
        final TextMessage someMessage = TextMessage.of("foo", Header.builder().withAttributes(of("attr", "value")).build(), "{}");
        final String encoded = encoder.apply(someMessage);
        assertThat(encoded).isEqualTo("{}");
    }

    @Test
    public void shouldEncodeInV2Format() {
        final TextEncoder encoder = new TextEncoder(MessageFormat.V2);
        final TextMessage someMessage = TextMessage.of(Key.of("foo", "bar"), Header.builder().withAttributes(of("attr", "value")).build(), "{}");
        final String encoded = encoder.apply(someMessage);
        assertThat(encoded).isEqualTo("{\"_synapse_msg_format\":\"v2\",\"_synapse_msg_key\":{\"partitionKey\":\"foo\",\"compactionKey\":\"bar\"},\"_synapse_msg_headers\":{\"attr\":\"value\"},\"_synapse_msg_payload\":{}}");
    }

}