package de.otto.synapse.endpoint.receiver.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static de.otto.synapse.channel.ChannelPosition.merge;
import static de.otto.synapse.info.MessageReceiverStatus.*;
import static de.otto.synapse.logging.LogHelper.info;
import static java.time.Clock.systemDefaultZone;
import static java.time.Duration.ofMillis;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class KafkaMessageLogReceiverEndpoint extends AbstractMessageLogReceiverEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageLogReceiverEndpoint.class);

    private static final long KAFKA_CONSUMER_POLLING_DURATION = 1000L;

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final ExecutorService executorService;
    private final ApplicationEventPublisher eventPublisher;
    private final MessageInterceptorRegistry interceptorRegistry;
    private final Clock clock;

    public KafkaMessageLogReceiverEndpoint(final String channelName,
                                           final MessageInterceptorRegistry interceptorRegistry,
                                           final KafkaConsumer<String, String> kafkaConsumer,
                                           final ExecutorService executorService,
                                           final ApplicationEventPublisher eventPublisher) {
        this(channelName, interceptorRegistry, kafkaConsumer, executorService, eventPublisher, systemDefaultZone());
    }

    public KafkaMessageLogReceiverEndpoint(final String channelName,
                                           final MessageInterceptorRegistry interceptorRegistry,
                                           final KafkaConsumer<String, String> kafkaConsumer,
                                           final ExecutorService executorService,
                                           final ApplicationEventPublisher eventPublisher,
                                           final Clock clock) {
        super(channelName, interceptorRegistry, eventPublisher);
        this.kafkaConsumer = kafkaConsumer;
        this.executorService = executorService;
        this.eventPublisher = eventPublisher;
        this.interceptorRegistry = interceptorRegistry;
        this.clock = clock;
    }

    @Nonnull
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull ChannelPosition startFrom,
                                                           final @Nonnull Predicate<ShardResponse> stopCondition) {
        publishEvent(STARTING, "Consuming messages from Kafka.", null);

        try {

            final ChannelDurationBehindHandler durationBehindHandler = new ChannelDurationBehindHandler(
                    getChannelName(),
                    eventPublisher
            );
            final ConsumerRebalanceHandler rebalanceHandler = new ConsumerRebalanceHandler(
                    getChannelName(),
                    startFrom,
                    eventPublisher,
                    kafkaConsumer
            );

            final KafkaRecordsConsumer recordsConsumer = new KafkaRecordsConsumer(
                    getChannelName(),
                    startFrom,
                    interceptorRegistry,
                    getMessageDispatcher(),
                    durationBehindHandler,
                    new KafkaDecoder()
            );

            final long t1 = System.currentTimeMillis();
            kafkaConsumer.subscribe(singletonList(getChannelName()), ConsumerRebalanceListeners.of(durationBehindHandler, rebalanceHandler));

            return supplyAsync(() ->
                    processMessages(startFrom, rebalanceHandler, recordsConsumer), executorService)
                    .thenApply((channelPosition -> {
                        final long t2 = System.currentTimeMillis();
                        info(LOG, ImmutableMap.of("runtime", (t2 - t1)), "Consume events from Kafka", null);
                        publishEvent(FINISHED, "Finished consuming messages from Kafka", null);
                        return channelPosition;
                    }))
                    .exceptionally((throwable) -> {
                        LOG.error("Failed to consume from Kafka stream {}: {}", getChannelName(), throwable.getMessage());
                        publishEvent(FAILED, "Failed to consume messages from Kafka: " + throwable.getMessage(), null);
                        // When an exception occurs in a completable future's thread, other threads continue running.
                        // Stop all before proceeding.
                        stop();
                        throw new RuntimeException(throwable.getMessage(), throwable);
                    });
        } catch (final RuntimeException e) {
            LOG.error("Failed to consume from Kafka stream {}: {}", getChannelName(), e.getMessage());
            throw e;
        } finally {
            kafkaConsumer.close();
        }
    }

    private ChannelPosition processMessages(@Nonnull ChannelPosition startFrom, ConsumerRebalanceHandler rebalanceHandler, KafkaRecordsConsumer recordsConsumer) {
        ChannelPosition channelPosition = startFrom;
        try {
            while (true) {
                final ConsumerRecords<String, String> records = kafkaConsumer.poll(ofMillis(KAFKA_CONSUMER_POLLING_DURATION));
                if (rebalanceHandler.shardsAssignedAndPositioned()) {
                    ChannelPosition newChannelPosition = recordsConsumer.apply(records);
                    channelPosition = merge(channelPosition, newChannelPosition);
                }
            }
        } catch (final WakeupException e) {
            // ignore for shutdown
            LOG.info("Shutting down Kafka consumer");
        }
        return channelPosition;
    }

    @Override
    public void stop() {
        LOG.info("Channel {} received stop signal.", getChannelName());
        kafkaConsumer.wakeup();
    }

    @VisibleForTesting
    List<PartitionInfo> getCurrentKafkaPartitions() {
        return kafkaConsumer.partitionsFor(getChannelName());
    }

}
