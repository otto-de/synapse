package de.otto.synapse.endpoint.receiver.kafka;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.TextMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.util.Map;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class KafkaDecoderTest {

    @Test
    public void shouldDecodeMessage() {
        // given
        final KafkaDecoder decoder = new KafkaDecoder();
        final ConsumerRecord<String,String> record = new ConsumerRecord<>(
                "ch01",
                0,
                42L,
                "key",
                "payload"
        );

        // when
        TextMessage decodedMessage = decoder.apply(record);

        // then
        assertThat(decodedMessage.getKey().compactionKey(), is("key"));
        assertThat(decodedMessage.getPayload(), is("payload"));
    }

    @Test
    public void shouldDecodeMessageWithNullKey() {
        // given
        final KafkaDecoder decoder = new KafkaDecoder();
        final ConsumerRecord<String,String> record = new ConsumerRecord<>(
                "ch01",
                0,
                42L,
                null,
                "payload"
        );

        // when
        TextMessage decodedMessage = decoder.apply(record);

        // then
        assertThat(decodedMessage.getKey().compactionKey(), notNullValue());
        assertThat(decodedMessage.getPayload(), is("payload"));
    }

    @Test
    public void shouldDecodeWithShardPositionHeader() {
        // given
        final KafkaDecoder decoder = new KafkaDecoder();
        final ConsumerRecord<String,String> record = new ConsumerRecord<>(
                "ch01",
                0,
                42L,
                "key",
                "payload"
        );

        // when
        TextMessage decodedMessage = decoder.apply(record);

        // then
        final ShardPosition expectedShardPosition = fromPosition("0", "42");
        final ShardPosition decodedShardPosition = decodedMessage.getHeader().getShardPosition().orElse(null);

        assertThat(decodedShardPosition, is(expectedShardPosition));
    }

    @Test
    public void shouldDecodeMessageHeaders() {
        // given
        final KafkaDecoder decoder = new KafkaDecoder();
        final ConsumerRecord<String,String> record = new ConsumerRecord<>(
                "ch01",
                0,
                42L,
                1234L, TimestampType.CREATE_TIME,
                -1L, -1, -1,
                "key",
                null,
                new RecordHeaders(asList(
                        new RecordHeader("foo", "foovalue".getBytes(UTF_8)),
                        new RecordHeader("bar", "barvalue".getBytes(UTF_8))
                ))
        );

        // when
        final TextMessage decodedMessage = decoder.apply(record);

        // then
        final Map<String, String> expectedHeaders = ImmutableMap.of(
                "foo", "foovalue",
                "bar", "barvalue"
        );
        assertThat(decodedMessage.getHeader().getAll(), is(expectedHeaders));
    }

    @Test
    public void shouldDecodeCompoundKeys() {
        final KafkaDecoder decoder = new KafkaDecoder();
        final ConsumerRecord<String,String> record = new ConsumerRecord<>(
                "ch01",
                0,
                42L,
                1234L, TimestampType.CREATE_TIME,
                -1L, -1, -1,
                "key-1234",
                null,
                new RecordHeaders(asList(
                        new RecordHeader("_synapse_msg_partitionKey", "1234".getBytes(UTF_8)),
                        new RecordHeader("_synapse_msg_compactionKey", "key-1234".getBytes(UTF_8))
                ))
        );

        // when
        final TextMessage decodedMessage = decoder.apply(record);

        // then
        assertThat(decodedMessage.getKey().isCompoundKey(), is(true));
        assertThat(decodedMessage.getKey().compactionKey(), is("key-1234"));
        assertThat(decodedMessage.getKey().partitionKey(), is("1234"));
    }

    @Test
    public void shouldDecodeBrokenCompoundKeysAsMessageKey() {
        final KafkaDecoder decoder = new KafkaDecoder();
        final ConsumerRecord<String,String> record = new ConsumerRecord<>(
                "ch01",
                0,
                42L,
                1234L, TimestampType.CREATE_TIME,
                -1L, -1, -1,
                "record-key",
                null,
                new RecordHeaders(asList(
                        new RecordHeader("_synapse_msg_partitionKey", "1234".getBytes(UTF_8)),
                        new RecordHeader("_synapse_msg_compactionKey", "key-1234".getBytes(UTF_8))
                ))
        );

        // when
        final TextMessage decodedMessage = decoder.apply(record);

        // then
        assertThat(decodedMessage.getKey().isCompoundKey(), is(false));
        assertThat(decodedMessage.getKey().compactionKey(), is("record-key"));
    }

}
