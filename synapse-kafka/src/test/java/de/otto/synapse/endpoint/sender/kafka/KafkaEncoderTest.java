package de.otto.synapse.endpoint.sender.kafka;

import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Test;

import java.time.Instant;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;

public class KafkaEncoderTest {


    @Test
    public void shouldEncodeMessage() {
        // given
        final KafkaEncoder encoder = new KafkaEncoder("test", 1);
        final TextMessage message = TextMessage.of("someKey", "payload");

        // when
        ProducerRecord<String, String> record = encoder.apply(message);

        // then
        assertThat(record.key(), is("someKey"));
        assertThat(record.value(), is("payload"));
        assertThat(record.headers(), is(emptyIterable()));
        assertThat(record.topic(), is("test"));
        assertThat(record.partition(), is(0));
    }

    @Test
    public void shouldEncodeMessageHeaders() {
        // given
        final KafkaEncoder encoder = new KafkaEncoder("test", 1);
        final TextMessage message = TextMessage.of(
                "someKey",
                Header.builder()
                        .withAttribute("foo", "bar")
                        .withAttribute("foobar", Instant.ofEpochMilli(42)).build(),
                null
        );

        // when
        final ProducerRecord<String, String> record = encoder.apply(message);

        // then
        assertThat(record.headers(), is(asList(
                new RecordHeader("foo", "bar".getBytes(UTF_8)),
                new RecordHeader("foobar", "42".getBytes(UTF_8))
        )));
    }

    @Test
    public void shouldPartitionMessage() {
        // given
        final KafkaEncoder encoder = new KafkaEncoder("test", 2);
        final TextMessage first = TextMessage.of(Key.of("0", "someKeyForPartition0"), null);
        final TextMessage second = TextMessage.of(Key.of("1", "someKeyForPartition1"), null);

        // when
        ProducerRecord<String, String> firstRecord = encoder.apply(first);
        ProducerRecord<String, String> secondRecord = encoder.apply(second);

        // then
        assertThat(firstRecord.key(), is("someKeyForPartition0"));
        assertThat(firstRecord.headers(), is(asList(
                new RecordHeader("_synapse_msg_partitionKey", "0".getBytes(UTF_8)),
                new RecordHeader("_synapse_msg_compactionKey", "someKeyForPartition0".getBytes(UTF_8))
        )));
        assertThat(firstRecord.partition(), is(0));
        // and
        assertThat(secondRecord.key(), is("someKeyForPartition1"));
        assertThat(secondRecord.headers(), is(asList(
                new RecordHeader("_synapse_msg_partitionKey", "1".getBytes(UTF_8)),
                new RecordHeader("_synapse_msg_compactionKey", "someKeyForPartition1".getBytes(UTF_8))
        )));
        assertThat(secondRecord.partition(), is(1));
    }

}