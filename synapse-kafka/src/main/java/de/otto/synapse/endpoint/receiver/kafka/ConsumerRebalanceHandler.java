package de.otto.synapse.endpoint.receiver.kafka;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Sets.newConcurrentHashSet;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.STARTED;
import static java.lang.Integer.parseInt;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.slf4j.LoggerFactory.getLogger;

class ConsumerRebalanceHandler implements ConsumerRebalanceListener {
    private static final Logger LOG = getLogger(ConsumerRebalanceHandler.class);

    private final String channelName;
    private final ChannelPosition channelPosition;
    private final ApplicationEventPublisher eventPublisher;
    private final Consumer<String, String> kafkaConsumer;
    private final Set<String> currentPartitions = newConcurrentHashSet();
    private final AtomicBoolean shardsAssignedAndPositioned = new AtomicBoolean(false);

    ConsumerRebalanceHandler(final String channelName,
                             final ChannelPosition startFrom,
                             final ApplicationEventPublisher eventPublisher,
                             final Consumer<String, String> kafkaConsumer) {
        this.channelName = channelName;
        this.channelPosition = startFrom;
        this.eventPublisher = eventPublisher;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        LOG.info("Revoked " + partitions + " Kafka partitions: " + partitions);
        partitions.forEach(p -> {
            final String shardName = "" + p.partition();
            currentPartitions.remove(shardName);
        });
        shardsAssignedAndPositioned.set(!currentPartitions.isEmpty());
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        LOG.info("Assigned " + partitions + " Kafka partitions: " + partitions);

        partitions.forEach(p -> currentPartitions.add("" + p.partition()));

        for (final TopicPartition partition: partitions) {
            final String shardName = "" + partition.partition();
            final ShardPosition shardPosition = channelPosition.shard(shardName);
            final TopicPartition topicPartition = new TopicPartition(channelName, partition.partition());
            switch (shardPosition.startFrom()) {
                case POSITION:
                    kafkaConsumer.seek(topicPartition, parseInt(shardPosition.position()) + 1);
                    break;
                case AT_POSITION:
                    kafkaConsumer.seek(topicPartition, parseInt(shardPosition.position()));
                    break;
                case HORIZON:
                    kafkaConsumer.seekToBeginning(singletonList(topicPartition));
                    break;
                case TIMESTAMP:
                    final Map<TopicPartition, Long> query = singletonMap(topicPartition, shardPosition.timestamp().toEpochMilli());
                    final Map<TopicPartition, OffsetAndTimestamp> offsets = kafkaConsumer.offsetsForTimes(query);
                    kafkaConsumer.seek(topicPartition, offsets.get(topicPartition).offset());
                    break;
            }
            LOG.info("Reading from channel={}, shard={}, position={}", channelName, shardName, shardPosition);
        }

        shardsAssignedAndPositioned.set(true);

        if (eventPublisher != null) {
            eventPublisher.publishEvent(builder()
                    .withChannelName(channelName)
                    .withStatus(STARTED)
                    .withMessage("Received shards from Kafka.")
                    .build());
        }

    }

    @Override
    public void onPartitionsLost(final Collection<TopicPartition> partitions) {
        LOG.warn("Lost " + partitions + " Kafka partitions: " + partitions);
        partitions.forEach(p -> {
            final String shardName = "" + p.partition();
            currentPartitions.remove(shardName);
        });
        shardsAssignedAndPositioned.set(!currentPartitions.isEmpty());
    }

    public Set<String> getCurrentPartitions() {
        return currentPartitions;
    }

    public boolean shardsAssignedAndPositioned() {
        return shardsAssignedAndPositioned.get();
    }

}
