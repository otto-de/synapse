package de.otto.synapse.endpoint.receiver.kafka;

import de.otto.synapse.channel.ChannelDurationBehind;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static de.otto.synapse.channel.ChannelDurationBehind.copyOf;
import static de.otto.synapse.channel.ChannelDurationBehind.unknown;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.RUNNING;
import static java.time.Clock.systemDefaultZone;
import static java.util.stream.Collectors.toSet;
import static org.slf4j.LoggerFactory.getLogger;

class ChannelDurationBehindHandler implements ConsumerRebalanceListener {
    private static final Logger LOG = getLogger(ChannelDurationBehindHandler.class);

    private final AtomicReference<ChannelDurationBehind> channelDurationBehind = new AtomicReference<>();
    private final String channelName;
    private final ApplicationEventPublisher eventPublisher;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Clock clock;

    ChannelDurationBehindHandler(final String channelName,
                                 final ApplicationEventPublisher eventPublisher,
                                 final KafkaConsumer<String, String> kafkaConsumer) {
        this(channelName, eventPublisher, systemDefaultZone(), kafkaConsumer);
    }

    ChannelDurationBehindHandler(final String channelName,
                                 final ApplicationEventPublisher eventPublisher,
                                 final Clock clock,
                                 final KafkaConsumer<String, String> kafkaConsumer) {
        this.channelName = channelName;
        this.eventPublisher = eventPublisher;
        this.kafkaConsumer = kafkaConsumer;
        this.channelDurationBehind.set(unknown());
        this.clock = clock;
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        LOG.info("Revoked " + partitions + " Kafka partitions: " + partitions);
        partitions.forEach(p -> {
            final String shardName = "" + p.partition();
            channelDurationBehind.getAndUpdate(previous -> copyOf(previous).without(shardName).build());
        });
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        final Set<String> shardNames = partitions
                .stream()
                .map(TopicPartition::partition)
                .map(String::valueOf)
                .collect(toSet());

        channelDurationBehind.getAndUpdate(previous -> copyOf(previous).withAllUnknown(shardNames).build());
    }

    public void update(final TopicPartition topicPartition, final long lastOffsetRead, final Instant lastTimestampRead) {
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(Collections.singletonList(topicPartition));

        Duration durationBehind;
        if (endOffsets.get(topicPartition) > lastOffsetRead) {
            durationBehind = Duration.between(lastTimestampRead, clock.instant());
        } else {
            durationBehind = Duration.ZERO;
        }

        Duration finalDurationBehind = durationBehind;
        channelDurationBehind.updateAndGet(behind -> copyOf(behind)
                .with("" + topicPartition.partition(), finalDurationBehind)
                .build());
        LOG.info("Read from '{}:{}', durationBehind={}", channelName, topicPartition.partition(), lastOffsetRead);

        if (eventPublisher != null) {
            eventPublisher.publishEvent(builder()
                    .withChannelName(channelName)
                    .withChannelDurationBehind(channelDurationBehind.get())
                    .withStatus(RUNNING)
                    .withMessage("Reading from Kafka stream.")
                    .build());
        }
    }

    ChannelDurationBehind getChannelDurationBehind() {
        return channelDurationBehind.get();
    }
}
