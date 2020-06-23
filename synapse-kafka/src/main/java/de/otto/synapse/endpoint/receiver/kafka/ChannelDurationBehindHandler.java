package de.otto.synapse.endpoint.receiver.kafka;

import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.channel.ChannelPosition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static de.otto.synapse.channel.ChannelDurationBehind.copyOf;
import static de.otto.synapse.channel.ChannelDurationBehind.unknown;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.RUNNING;
import static java.time.Clock.systemDefaultZone;
import static org.slf4j.LoggerFactory.getLogger;

class ChannelDurationBehindHandler implements ConsumerRebalanceListener {
    private static final Logger LOG = getLogger(ChannelDurationBehindHandler.class);

    private final List<TopicPartition> partitions = new CopyOnWriteArrayList<>();
    private final AtomicReference<ChannelDurationBehind> channelDurationBehind = new AtomicReference<>();
    private final String channelName;
    private final ChannelPosition startFrom;
    private final ApplicationEventPublisher eventPublisher;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Clock clock;

    ChannelDurationBehindHandler(final String channelName,
                                 final ChannelPosition startFrom,
                                 final ApplicationEventPublisher eventPublisher,
                                 final KafkaConsumer<String, String> kafkaConsumer) {
        this(channelName, startFrom, eventPublisher, systemDefaultZone(), kafkaConsumer);
    }

    ChannelDurationBehindHandler(final String channelName,
                                 final ChannelPosition startFrom,
                                 final ApplicationEventPublisher eventPublisher,
                                 final Clock clock,
                                 final KafkaConsumer<String, String> kafkaConsumer) {
        this.channelName = channelName;
        this.startFrom = startFrom;
        this.eventPublisher = eventPublisher;
        this.kafkaConsumer = kafkaConsumer;

        //when startposition => endposition => duration behind = 0S
        this.channelDurationBehind.set(unknown());
        this.clock = clock;
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        LOG.info("Revoked " + partitions + " Kafka partitions: " + partitions);
        this.partitions.removeAll(partitions);
        partitions.forEach(p -> {
            final String shardName = "" + p.partition();
            channelDurationBehind.getAndUpdate(previous -> copyOf(previous).without(shardName).build());
        });
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        this.partitions.addAll(partitions);
        channelDurationBehind.updateAndGet(channelDurationBehind -> updateOnPartitionChanged(startFrom, partitions, channelDurationBehind));
    }

    public void update(final TopicPartition topicPartition, final long lastOffsetRead, final Instant lastTimestampRead) {
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(Collections.singletonList(topicPartition));

        Duration durationBehind;
        if (endOffsets.get(topicPartition) - 1 > lastOffsetRead) {
            durationBehind = Duration.between(lastTimestampRead, clock.instant());
        } else {
            durationBehind = Duration.ZERO;
        }

        Duration finalDurationBehind = durationBehind;
        channelDurationBehind.updateAndGet(behind -> copyOf(behind)
                .with("" + topicPartition.partition(), finalDurationBehind)
                .build());
        LOG.debug("Read from '{}:{}', durationBehind={}", channelName, topicPartition.partition(), durationBehind);

        if (eventPublisher != null) {
            eventPublisher.publishEvent(builder()
                    .withChannelName(channelName)
                    .withChannelDurationBehind(channelDurationBehind.get())
                    .withStatus(RUNNING)
                    .withMessage("Reading from Kafka stream.")
                    .build());
        }
    }

    private ChannelDurationBehind updateOnPartitionChanged(ChannelPosition startFrom, final Collection<TopicPartition> partitions, ChannelDurationBehind channelDurationBehind) {
        Map<TopicPartition, Long> topicPartitionToEndOffsetMap = this.kafkaConsumer.endOffsets(partitions);
        ChannelDurationBehind.Builder channelDurationBehindBuilder = copyOf(channelDurationBehind);
        topicPartitionToEndOffsetMap.forEach((topicPartition, endOffset) -> {
            String shard = "" + topicPartition.partition();
            String startPosition = startFrom.shard(shard).position();
            long startPos = startPosToLong(startPosition);
            if (startPos >= endOffset) {
                channelDurationBehindBuilder.with(shard, Duration.ZERO);
            } else {
                channelDurationBehindBuilder.with(shard, Duration.ofMillis(Long.MAX_VALUE));
            }
        });
        return channelDurationBehindBuilder.build();
    }

    private long startPosToLong(String startPosition) {
        if (startPosition == null || startPosition.equals("")) {
            return  0;
        } else {
            return Long.parseLong(startPosition);
        }
    }

    ChannelDurationBehind getChannelDurationBehind() {
        return channelDurationBehind.get();
    }

}
