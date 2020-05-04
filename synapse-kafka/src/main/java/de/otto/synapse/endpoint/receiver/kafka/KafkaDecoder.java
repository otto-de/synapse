package de.otto.synapse.endpoint.receiver.kafka;

import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.Decoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.slf4j.LoggerFactory.getLogger;

public class KafkaDecoder implements Decoder<ConsumerRecord<String, String>> {

    private static final Logger LOG = getLogger(KafkaDecoder.class);

    public static String COMPACTION_KEY = "_synapse_msg_compactionKey";
    public static String PARTITION_KEY = "_synapse_msg_partitionKey";

    @Override
    public TextMessage apply(final ConsumerRecord<String, String> record) {
        return TextMessage.of(
                toKey(record),
                toHeader(record),
                record.value());
    }

    private Key toKey(final ConsumerRecord<String, String> record) {
        final Key key;
        final Headers headers = record.headers();

        final String partitionKey = lastHeader(headers, PARTITION_KEY);
        final String compactionKey = lastHeader(headers, COMPACTION_KEY);

        if (partitionKey != null && compactionKey != null && compactionKey.equals(record.key())) {
            key = Key.of(partitionKey, compactionKey);
        } else {
            key = Key.of(record.key());
        }
        return key;
    }

    private de.otto.synapse.message.Header toHeader(final ConsumerRecord<String, String> record) {
        de.otto.synapse.message.Header.Builder builder = de.otto.synapse.message.Header.builder()
                .withShardPosition(fromPosition("" + record.partition(), "" + (record.offset())));
        record.headers().forEach(header -> {
            builder.withAttribute(header.key(), toString(header.value()));
        });
        return builder.build();
    }

    private String lastHeader(final Headers headers, final String key) {
        final Header header = headers.lastHeader(key);
        return header != null ? toString(header.value()) : null;
    }

    private String toString(final byte[] bytes) {
        return new String(bytes, UTF_8);
    }
}
