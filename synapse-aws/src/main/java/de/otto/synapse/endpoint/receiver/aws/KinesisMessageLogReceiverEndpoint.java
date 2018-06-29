package de.otto.synapse.endpoint.receiver.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static de.otto.synapse.channel.ChannelDurationBehind.copyOf;
import static de.otto.synapse.channel.ChannelDurationBehind.unknown;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.*;
import static de.otto.synapse.logging.LogHelper.info;

public class KinesisMessageLogReceiverEndpoint extends AbstractMessageLogReceiverEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisMessageLogReceiverEndpoint.class);


    private static class KinesisShardResponseConsumer implements Consumer<KinesisShardResponse> {
        private final AtomicReference<ChannelDurationBehind> channelDurationBehind = new AtomicReference<>();
        private final InterceptorChain interceptorChain;
        private final MessageDispatcher messageDispatcher;
        private final ApplicationEventPublisher eventPublisher;

        private KinesisShardResponseConsumer(final List<String> shardNames,
                                             final InterceptorChain interceptorChain,
                                             final MessageDispatcher messageDispatcher,
                                             final ApplicationEventPublisher eventPublisher) {
            this.interceptorChain = interceptorChain;
            this.messageDispatcher = messageDispatcher;
            this.eventPublisher = eventPublisher;
            channelDurationBehind.set(unknown(shardNames));
        }

        @Override
        public void accept(KinesisShardResponse response) {
            response.getMessages().forEach(message -> {
                try {
                    final Message<String> interceptedMessage = interceptorChain.intercept(message);
                    if (interceptedMessage != null) {
                        messageDispatcher.accept(message);
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
                        .withChannelName(response.getChannelName())
                        .withChannelDurationBehind(channelDurationBehind.get())
                        .withStatus(RUNNING)
                        .withMessage("Reading from kinesis shard.")
                        .build());
            }

        }

    }

    private final KinesisMessageLogReader kinesisMessageLogReader;
    private final ApplicationEventPublisher eventPublisher;


    public KinesisMessageLogReceiverEndpoint(final String channelName,
                                             final KinesisClient kinesisClient,
                                             final ObjectMapper objectMapper,
                                             final ApplicationEventPublisher eventPublisher) {
        this(channelName, kinesisClient, objectMapper, eventPublisher, Clock.systemDefaultZone());
    }

    public KinesisMessageLogReceiverEndpoint(final String channelName,
                                             final KinesisClient kinesisClient,
                                             final ObjectMapper objectMapper,
                                             final ApplicationEventPublisher eventPublisher,
                                             final Clock clock) {
        super(channelName, objectMapper, eventPublisher);
        this.eventPublisher = eventPublisher;
        this.kinesisMessageLogReader = new KinesisMessageLogReader(channelName, kinesisClient, clock);
    }

    @Override
    @Nonnull
    public ChannelPosition consumeUntil(final @Nonnull ChannelPosition startFrom,
                                        final @Nonnull Instant until) {
        try {
            publishEvent(STARTING, "Consuming messages from Kinesis.", null);
            final long t1 = System.currentTimeMillis();

            final List<String> shards = kinesisMessageLogReader.getOpenShards();

            publishEvent(STARTED, "Received shards from Kinesis.", null);

            final KinesisShardResponseConsumer consumer = new KinesisShardResponseConsumer(shards, getInterceptorChain(), getMessageDispatcher(), eventPublisher);

            final CompletableFuture<ChannelPosition> futureChannelPosition = kinesisMessageLogReader.consumeUntil(startFrom, until, consumer);

            return futureChannelPosition
                    .exceptionally((throwable) -> {
                        LOG.error("Failed to consume from Kinesis stream {}: {}", getChannelName(), throwable.getMessage());
                        publishEvent(FAILED, "Failed to consume messages from Kinesis: " + throwable.getMessage(), null);
                        // When an exception occurs in a completable future's thread, other threads continue running.
                        // Stop all before proceeding.
                        stop();
                        throw new RuntimeException(throwable.getMessage(), throwable);
                    })
                    .thenApply((channelPosition -> {
                        final long t2 = System.currentTimeMillis();
                        info(LOG, ImmutableMap.of("runtime", (t2-t1)), "Consume events from Kinesis", null);
                        publishEvent(FINISHED, "Finished consuming messages from Kinesis", null);
                        return channelPosition;
                    }))
                    .get();
        } catch (final Exception e) {
            LOG.error("Failed to consume from Kinesis stream {}: {}", getChannelName(), e.getMessage());
            publishEvent(FAILED, "Failed to consume messages from Kinesis: " + e.getMessage(), null);
            // When an exception occurs in a completable future's thread, other threads continue running.
            // Stop all before proceeding.
            stop();
            throw new RuntimeException(e.getMessage(), e);
        }
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
