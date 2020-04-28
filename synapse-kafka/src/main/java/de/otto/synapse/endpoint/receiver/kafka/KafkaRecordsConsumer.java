package de.otto.synapse.endpoint.receiver.kafka;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.TextMessage;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Maps.newHashMap;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.endpoint.EndpointType.RECEIVER;
import static java.lang.Integer.parseInt;
import static org.slf4j.LoggerFactory.getLogger;

class KafkaRecordsConsumer implements Function<ConsumerRecords<String, String>, ChannelPosition> {
    private static final Logger LOG = getLogger(KafkaRecordsConsumer.class);

    private final MessageInterceptorRegistry interceptorRegistry;
    private final String channelName;
    private final MessageDispatcher messageDispatcher;
    private final ChannelDurationBehindHandler durationBehindHandler;
    private final KafkaDecoder decoder;
    private ChannelPosition channelPosition;

    KafkaRecordsConsumer(final String channelName,
                         final ChannelPosition startFrom,
                         final MessageInterceptorRegistry interceptorRegistry,
                         final MessageDispatcher messageDispatcher,
                         final ChannelDurationBehindHandler durationBehindHandler,
                         final KafkaDecoder decoder) {
        this.channelName = channelName;
        this.channelPosition = startFrom;
        this.messageDispatcher = messageDispatcher;
        this.interceptorRegistry = interceptorRegistry;
        this.durationBehindHandler = durationBehindHandler;
        this.decoder = decoder;
    }

    @Override
    public ChannelPosition apply(final ConsumerRecords<String, String> records) {
        final InterceptorChain interceptorChain = interceptorRegistry.getInterceptorChain(channelName, RECEIVER);
        final Map<String, String> shardPositions = newHashMap();
        records.forEach(record -> {
            try {
                final TextMessage message = decoder.apply(record);
                LOG.debug("Processing message " + message.getKey());
                final TextMessage interceptedMessage = interceptorChain.intercept(message);
                if (interceptedMessage != null) {
                    messageDispatcher.accept(interceptedMessage);
                    shardPositions.put("" + record.partition(), "" + record.offset());
                } else {
                    LOG.debug("Message {} dropped by interceptor", message.getKey());
                }
            } catch (final Exception e) {
                LOG.error("Error processing message: " + e.getMessage(), e);

                // TODO: records in dead-letter queue?!

            }
        });

        final long now = Clock.systemUTC().millis();
        for (final TopicPartition topicPartition : records.partitions()) {
            final long partitionBehind = now - getLast(records.records(topicPartition)).timestamp();
            durationBehindHandler.update("" + topicPartition.partition(), Duration.ofMillis(partitionBehind));
        }

        for (final Map.Entry<String, String> entry : shardPositions.entrySet()) {
            final ShardPosition pos = fromPosition(entry.getKey(), followingShardPosition(entry.getValue()));
            channelPosition = ChannelPosition.merge(channelPosition, pos);
        }

        return channelPosition;
    }

    private String followingShardPosition(final String currentPosition) {
        return "" + (parseInt(currentPosition) + 1);
    }

}
