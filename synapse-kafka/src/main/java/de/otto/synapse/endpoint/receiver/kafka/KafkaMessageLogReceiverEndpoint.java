package de.otto.synapse.endpoint.receiver.kafka;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static de.otto.synapse.info.MessageReceiverStatus.*;
import static de.otto.synapse.logging.LogHelper.info;
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

    public KafkaMessageLogReceiverEndpoint(final String channelName,
                                           final MessageInterceptorRegistry interceptorRegistry,
                                           final KafkaConsumer<String, String> kafkaConsumer,
                                           final ExecutorService executorService,
                                           final ApplicationEventPublisher eventPublisher) {
        super(channelName, interceptorRegistry, eventPublisher);
        this.kafkaConsumer = kafkaConsumer;
        this.executorService = executorService;
        this.eventPublisher = eventPublisher;
        this.interceptorRegistry = interceptorRegistry;
    }

    @Nonnull
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull ChannelPosition startFrom,
                                                           final @Nonnull Predicate<ShardResponse> stopCondition) {
        publishEvent(STARTING, "Consuming messages from Kafka.", null);

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
                rebalanceHandler::getCurrentPartitions,
                new KafkaDecoder()
        );

        final long t1 = System.currentTimeMillis();
        final Set<String> subscription = kafkaConsumer.subscription();
        if (!subscription.isEmpty()) {
            if (!subscription.contains(getChannelName())) {
                LOG.error("KafkaConsumer is already subscribed to " + subscription);
                throw new IllegalStateException("Unable to consume channel " + getChannelName() + " using KafkaConsumer that is subscribed to " + subscription);
            }
            kafkaConsumer.unsubscribe();
        }
        kafkaConsumer.subscribe(singletonList(getChannelName()), ConsumerRebalanceListeners.of(durationBehindHandler, rebalanceHandler));

        return supplyAsync(() -> processMessages(startFrom, stopCondition, rebalanceHandler, recordsConsumer), executorService)
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
    }

    private ChannelPosition processMessages(final ChannelPosition startFrom,
                                            final Predicate<ShardResponse> stopCondition,
                                            final ConsumerRebalanceHandler rebalanceHandler,
                                            final KafkaRecordsConsumer recordsConsumer) {
        ChannelPosition channelPosition = startFrom;
        boolean stopConditionMet = false;
        try {
            do {
                final ConsumerRecords<String, String> records = kafkaConsumer.poll(ofMillis(KAFKA_CONSUMER_POLLING_DURATION));
                if (rebalanceHandler.shardsAssignedAndPositioned()) {
                    final ChannelResponse channelResponse = recordsConsumer.apply(records);
                    channelPosition = channelResponse.getChannelPosition();
                    stopConditionMet = channelResponse.getShardResponses().stream().anyMatch(stopCondition);
                    kafkaConsumer.commitAsync();
                }
            } while (!stopConditionMet);
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

}
