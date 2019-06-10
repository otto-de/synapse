package de.otto.synapse.endpoint.receiver.kinesis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import de.otto.synapse.message.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static de.otto.synapse.channel.ChannelDurationBehind.copyOf;
import static de.otto.synapse.channel.ChannelDurationBehind.unknown;
import static de.otto.synapse.endpoint.EndpointType.RECEIVER;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.*;
import static de.otto.synapse.logging.LogHelper.info;

public class KinesisMessageLogReceiverEndpoint extends AbstractMessageLogReceiverEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisMessageLogReceiverEndpoint.class);

    private static class KinesisShardResponseConsumer implements Consumer<ShardResponse> {
        private final AtomicReference<ChannelDurationBehind> channelDurationBehind = new AtomicReference<>();
        private final MessageInterceptorRegistry interceptorRegistry;
        private final String channelName;
        private final MessageDispatcher messageDispatcher;
        private final ApplicationEventPublisher eventPublisher;

        private KinesisShardResponseConsumer(final String channelName,
                                             final List<String> shardNames,
                                             final MessageInterceptorRegistry interceptorRegistry,
                                             final MessageDispatcher messageDispatcher,
                                             final ApplicationEventPublisher eventPublisher) {
            this.channelName = channelName;
            this.messageDispatcher = messageDispatcher;
            this.interceptorRegistry = interceptorRegistry;
            this.eventPublisher = eventPublisher;
            channelDurationBehind.set(unknown(shardNames));
        }

        @Override
        public void accept(final ShardResponse response) {
            final InterceptorChain interceptorChain = interceptorRegistry.getInterceptorChain(channelName, RECEIVER);
            response.getMessages().forEach(message -> {
                try {
                    LOG.debug("Processing message " + message.getKey());
                    final TextMessage interceptedMessage = interceptorChain.intercept(message);
                    if (interceptedMessage != null) {
                        messageDispatcher.accept(interceptedMessage);
                    } else {
                        LOG.debug("Message {} dropped by interceptor", message.getKey());
                    }
                } catch (final Exception e) {
                    LOG.error("Error processing message: " + e.getMessage(), e);
                }
            });
            channelDurationBehind.updateAndGet(behind -> copyOf(behind)
                    .with(response.getShardName(), response.getDurationBehind())
                    .build());

            if (eventPublisher != null) {
                eventPublisher.publishEvent(builder()
                        .withChannelName(channelName)
                        .withChannelDurationBehind(channelDurationBehind.get())
                        .withStatus(RUNNING)
                        .withMessage("Reading from kinesis shard.")
                        .build());
            }

        }

    }

    private final KinesisMessageLogReader kinesisMessageLogReader;
    private final ApplicationEventPublisher eventPublisher;
    private final MessageInterceptorRegistry interceptorRegistry;


    public KinesisMessageLogReceiverEndpoint(final String channelName,
                                             final MessageInterceptorRegistry interceptorRegistry,
                                             final KinesisAsyncClient kinesisClient,
                                             final ExecutorService executorService,
                                             final ApplicationEventPublisher eventPublisher) {
        this(channelName, interceptorRegistry, kinesisClient, executorService, eventPublisher, Clock.systemDefaultZone());
    }

    public KinesisMessageLogReceiverEndpoint(final String channelName,
                                             final MessageInterceptorRegistry interceptorRegistry,
                                             final KinesisAsyncClient kinesisClient,
                                             final ExecutorService executorService,
                                             final ApplicationEventPublisher eventPublisher,
                                             final Clock clock) {
        super(channelName, interceptorRegistry, eventPublisher);
        this.eventPublisher = eventPublisher;
        this.kinesisMessageLogReader = new KinesisMessageLogReader(channelName, kinesisClient, executorService, clock);
        this.interceptorRegistry = interceptorRegistry;
    }

    public KinesisMessageLogReceiverEndpoint(final String channelName,
                                             final MessageInterceptorRegistry interceptorRegistry,
                                             final KinesisAsyncClient kinesisClient,
                                             final ExecutorService executorService,
                                             final ApplicationEventPublisher eventPublisher,
                                             final Clock clock,
                                             final int waitingTimeOnEmptyRecords,
                                             final Marker marker) {
        super(channelName, interceptorRegistry, eventPublisher);
        this.eventPublisher = eventPublisher;
        this.kinesisMessageLogReader = new KinesisMessageLogReader(channelName, kinesisClient, executorService, clock, waitingTimeOnEmptyRecords, marker);
        this.interceptorRegistry = interceptorRegistry;
    }

    @Nonnull
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull ChannelPosition startFrom,
                                                           final @Nonnull Predicate<ShardResponse> stopCondition) {
            publishEvent(STARTING, "Consuming messages from Kinesis.", null);
            final long t1 = System.currentTimeMillis();
            final List<String> shards = kinesisMessageLogReader.getOpenShards();

            publishEvent(STARTED, "Received shards from Kinesis.", null);

            final KinesisShardResponseConsumer consumer = new KinesisShardResponseConsumer(getChannelName(), shards, interceptorRegistry, getMessageDispatcher(), eventPublisher);

            return kinesisMessageLogReader.consumeUntil(startFrom, stopCondition, consumer)
                    .thenApply((channelPosition -> {
                        final long t2 = System.currentTimeMillis();
                        info(LOG, ImmutableMap.of("runtime", (t2-t1)), "Consume events from Kinesis", null);
                        publishEvent(FINISHED, "Finished consuming messages from Kinesis", null);
                        return channelPosition;
                    }))
                    .exceptionally((throwable) -> {
                        LOG.error("Failed to consume from Kinesis stream {}: {}", getChannelName(), throwable.getMessage());
                        publishEvent(FAILED, "Failed to consume messages from Kinesis: " + throwable.getMessage(), null);
                        // When an exception occurs in a completable future's thread, other threads continue running.
                        // Stop all before proceeding.
                        stop();
                        throw new RuntimeException(throwable.getMessage(), throwable);
                    });
    }

    @Override
    public void stop() {
        LOG.info("Channel {} received stop signal.", getChannelName());
        kinesisMessageLogReader.stop();
    }

    @VisibleForTesting
    List<KinesisShardReader> getCurrentKinesisShards() {
        return kinesisMessageLogReader.getCurrentKinesisShards();
    }


}
