package de.otto.synapse.endpoint.receiver.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.TextMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.builder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Maps.newHashMap;
import static de.otto.synapse.channel.ChannelPosition.merge;
import static de.otto.synapse.channel.ChannelResponse.channelResponse;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.ShardResponse.shardResponse;
import static de.otto.synapse.endpoint.EndpointType.RECEIVER;
import static java.lang.Integer.parseInt;
import static java.time.Duration.ofMillis;
import static org.slf4j.LoggerFactory.getLogger;

class KafkaRecordsConsumer implements Function<ConsumerRecords<String, String>, ChannelResponse> {
    private static final Logger LOG = getLogger(KafkaRecordsConsumer.class);
    public static final Duration UNKNOWN_DURATION_BEHIND = ofMillis(Long.MAX_VALUE);

    private final String channelName;
    private final MessageInterceptorRegistry interceptorRegistry;
    private final MessageDispatcher messageDispatcher;
    private final Supplier<Set<String>> currentShardsSupplier;
    private final ChannelDurationBehindHandler durationBehindHandler;
    private final KafkaDecoder decoder;
    private ChannelPosition currentChannelPosition;

    KafkaRecordsConsumer(final String channelName,
                         final ChannelPosition startFrom,
                         final MessageInterceptorRegistry interceptorRegistry,
                         final MessageDispatcher messageDispatcher,
                         final ChannelDurationBehindHandler durationBehindHandler,
                         final Supplier<Set<String>> currentShardsSupplier,
                         final KafkaDecoder decoder) {
        this.channelName = channelName;
        this.currentChannelPosition = startFrom;
        this.interceptorRegistry = interceptorRegistry;
        this.messageDispatcher = messageDispatcher;
        this.currentShardsSupplier = currentShardsSupplier;
        this.durationBehindHandler = durationBehindHandler;
        this.decoder = decoder;
    }

    @Override
    public ChannelResponse apply(final ConsumerRecords<String, String> records) {
        final Map<String, ImmutableList.Builder<TextMessage>> receivedMessagesPerShard = newHashMap();
        final Map<String, ShardPosition> shardPositionsFromRecords = newHashMap();
        records.forEach(record -> {
            try {
                final String shardName = "" + record.partition();
                final TextMessage message = decoder.apply(record);
                LOG.debug("Processing message " + message.getKey());
                final TextMessage interceptedMessage = interceptorRegistry
                        .getInterceptorChain(channelName, RECEIVER)
                        .intercept(message);
                shardPositionsFromRecords.put(shardName, toShardPosition(record));
                if (interceptedMessage != null) {
                    messageDispatcher.accept(interceptedMessage);
                    receivedMessagesPerShard.compute(shardName, (key, messages) -> {
                        if (messages != null) {
                            return messages.add(interceptedMessage);
                        } else {
                            return ImmutableList.<TextMessage>builder().add(interceptedMessage);
                        }
                    });
                } else {
                    LOG.debug("Message {} dropped by interceptor", message.getKey());
                }
            } catch (final Exception e) {
                LOG.error("Error processing message: " + e.getMessage(), e);

                // TODO: records in dead-letter queue?!

            }
        });

        final ImmutableMap<String, Duration> channelDurationBehind = updateAndGetDurationBehind(records);

        updateCurrentChannelPosition(shardPositionsFromRecords.values());

        return channelResponse(channelName, currentShardPositions()
                .stream()
                .map(shardPosition -> {
                    final String shardName = shardPosition.shardName();
                    final ImmutableList<TextMessage> messages = receivedMessagesPerShard.getOrDefault(shardName, builder()).build();
                    final Duration durationBehind = channelDurationBehind.getOrDefault(shardName, UNKNOWN_DURATION_BEHIND);
                    return shardResponse(
                            shardPosition,
                            durationBehind,
                            messages
                    );
                })
                .collect(toImmutableList()));
    }

    private ShardPosition toShardPosition(final ConsumerRecord<String, String> record) {
        return fromPosition("" + record.partition(), followingShardPosition("" + (record.offset())));
    }

    private ImmutableList<ShardPosition> currentShardPositions() {
        return currentShardsSupplier.get()
                .stream()
                .map(shardName -> currentChannelPosition.shard(shardName))
                .collect(toImmutableList());
    }

    private void updateCurrentChannelPosition(final Collection<ShardPosition> shardPositionsFromRecords) {
        shardPositionsFromRecords.forEach((shardPos) -> currentChannelPosition = merge(currentChannelPosition, shardPos));
    }

    private ImmutableMap<String, Duration> updateAndGetDurationBehind(ConsumerRecords<String, String> records) {
        for (final TopicPartition topicPartition : records.partitions()) {
            ConsumerRecord<String, String> lastRecord = getLast(records.records(topicPartition));
            final Instant lastTimestampRead = Instant.ofEpochMilli(lastRecord.timestamp());
            durationBehindHandler.update(topicPartition, lastRecord.offset(), lastTimestampRead);
        }

        return durationBehindHandler
                .getChannelDurationBehind()
                .getShardDurationsBehind();
    }

    private String followingShardPosition(final String currentPosition) {
        return "" + (parseInt(currentPosition));
    }

}
