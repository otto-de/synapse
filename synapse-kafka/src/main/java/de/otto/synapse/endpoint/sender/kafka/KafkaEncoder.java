package de.otto.synapse.endpoint.sender.kafka;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.Encoder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.utils.Utils.murmur2;
import static org.apache.kafka.common.utils.Utils.toPositive;

public class KafkaEncoder implements Encoder<ProducerRecord<String,String>> {
    public static String COMPACTION_KEY = "_synapse_msg_compactionKey";
    public static String PARTITION_KEY = "_synapse_msg_partitionKey";

    private final String channelName;
    private final int numPartitions;

    public KafkaEncoder(final String channelName, final int numPartitions) {
        this.channelName = channelName;
        this.numPartitions = numPartitions;
    }

    @Override
    public ProducerRecord<String, String> apply(Message<String> message) {
        final Key key = message.getKey();
        final Integer partition = key.isCompoundKey()
                ? kafkaPartitionFrom(key.partitionKey())
                : null;

        return new ProducerRecord<>(
                channelName,
                partition,
                key.compactionKey(),
                message.getPayload(),
                headersOf(message)
        );
    }

    private List<Header> headersOf(final Message<String> message) {
        final ImmutableList.Builder<org.apache.kafka.common.header.Header> messageAttributes = ImmutableList.builder();
        message.getHeader()
                .getAll()
                .forEach((key, value) -> messageAttributes.add(new RecordHeader(key, value.getBytes(UTF_8))));
        messageAttributes.add(
                new RecordHeader(PARTITION_KEY, message.getKey().partitionKey().getBytes(UTF_8)),
                new RecordHeader(COMPACTION_KEY, message.getKey().compactionKey().getBytes(UTF_8))
        );
        return messageAttributes.build();
    }

    private int kafkaPartitionFrom(final String partitionKey) {
        //final int numPartitions = kafkaTemplate.partitionsFor(getChannelName()).size();
        return toPositive(murmur2(partitionKey.getBytes())) % numPartitions;
    }

}
