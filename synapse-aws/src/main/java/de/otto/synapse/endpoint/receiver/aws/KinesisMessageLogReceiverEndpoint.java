package de.otto.synapse.endpoint.receiver.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static de.otto.synapse.channel.ChannelDurationBehind.copyOf;
import static de.otto.synapse.channel.ChannelDurationBehind.unknown;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.*;
import static de.otto.synapse.logging.LogHelper.info;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toList;

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

    private final KinesisClient kinesisClient;
    private final Clock clock;
    private List<KinesisShardReader> kinesisShardReaders;
    private ExecutorService executorService;
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
        this.kinesisClient = kinesisClient;
        this.clock = clock;
        this.eventPublisher = eventPublisher;
        initExecutorService();
    }

    @Override
    @Nonnull
    public ChannelPosition consumeUntil(final @Nonnull ChannelPosition startFrom,
                                        final @Nonnull Instant until) {
        try {
            publishEvent(STARTING, "Consuming messages from Kinesis.", null);
            final long t1 = System.currentTimeMillis();
            if (isNull(executorService)) {
               initExecutorService();
            }
            final List<String> shards = kinesisShardReaders.stream()
                    .map(KinesisShardReader::getShardName)
                    .collect(toList());


            publishEvent(STARTED, "Received shards from Kinesis.", null);

            final KinesisShardResponseConsumer consumer = new KinesisShardResponseConsumer(shards, getInterceptorChain(), getMessageDispatcher(), eventPublisher);

            final List<CompletableFuture<ShardPosition>> futureShardPositions = kinesisShardReaders
                    .stream()
                    .map(shard -> supplyAsync(() -> shard.consumeUntil(startFrom.shard(shard.getShardName()), until, consumer), executorService))
                    .collect(toList());

            // don't chain futureShardPositions with CompletableFuture::join as lazy execution will prevent threads from
            // running in parallel

            final List<ShardPosition> shardPositions = futureShardPositions
                    .stream()
                    .map(CompletableFuture::join)
                    .collect(toList());
            final long t2 = System.currentTimeMillis();
            info(LOG, ImmutableMap.of("runtime", (t2-t1)), "Consume events from Kinesis", null);
            publishEvent(FINISHED, "Finished consuming messages from Kinesis", null);
            return channelPosition(shardPositions);
        } catch (final RuntimeException e) {
            LOG.error("Failed to consume from Kinesis stream {}: {}", getChannelName(), e.getMessage());
            publishEvent(FAILED, "Failed to consume messages from Kinesis: " + e.getMessage(), null);
            // When an exception occurs in a completable future's thread, other threads continue running.
            // Stop all before proceeding.
            stop();
            executorService.shutdownNow();
            try {
                boolean allThreadsSafelyTerminated = executorService.awaitTermination(30, TimeUnit.SECONDS);
                if (!allThreadsSafelyTerminated) {
                    LOG.error("Kinesis Thread for stream {} is still running", getChannelName());
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            executorService = null;
            throw e;
        }
    }

    private void initExecutorService() {
        this.kinesisShardReaders = retrieveAllOpenShards();
        if (kinesisShardReaders.isEmpty()) {
            this.executorService = newSingleThreadExecutor();
        } else {
            this.executorService = newFixedThreadPool(kinesisShardReaders.size(),
                    new ThreadFactoryBuilder().setNameFormat("kinesis-message-log-%d").build());
        }
    }

    @Override
    public void stop() {
        this.kinesisShardReaders.forEach(KinesisShardReader::stop);
    }

    @VisibleForTesting
    List<KinesisShardReader> getCurrentKinesisShards() {
        return kinesisShardReaders;
    }

    private List<KinesisShardReader> retrieveAllOpenShards() {
        return retrieveAllShards()
                .stream()
                .filter(this::isShardOpen)
                .map(shard -> {
                    return new KinesisShardReader(getChannelName(), shard.shardId(), kinesisClient, clock);
                })
                .collect(toImmutableList());
    }

    private List<Shard> retrieveAllShards() {
        List<Shard> shardList = new ArrayList<>();

        boolean fetchMore = true;
        while (fetchMore) {
            fetchMore = retrieveAndAppendNextBatchOfShards(shardList);
        }
        return shardList;
    }

    private boolean retrieveAndAppendNextBatchOfShards(List<Shard> shardList) {
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest
                .builder()
                .streamName(getChannelName())
                .exclusiveStartShardId(getLastSeenShardId(shardList))
                .limit(10)
                .build();

        DescribeStreamResponse describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
        shardList.addAll(describeStreamResult.streamDescription().shards());

        return describeStreamResult.streamDescription().hasMoreShards();
    }

    private String getLastSeenShardId(List<Shard> shardList) {
        if (!shardList.isEmpty()) {
            return shardList.get(shardList.size() - 1).shardId();
        } else {
            return null;
        }
    }

    private boolean isShardOpen(Shard shard) {
        if (shard.sequenceNumberRange().endingSequenceNumber() == null) {
            return true;
        } else {
            LOG.warn("Shard with id {} is closed. Cannot retrieve data.", shard.shardId());
            return false;
        }
    }

    private static void logInfo(final String channelName, final KinesisShardResponse response, final Duration durationBehind, final long runtime) {
        int recordCount = response.getMessages().size();
        boolean isBehind = durationBehind.getSeconds() > 0;
        if (recordCount > 0 || isBehind) {
            final String durationString = format("%s days %s hrs %s min %s sec", durationBehind.toDays(), durationBehind.toHours() % 24, durationBehind.toMinutes() % 60, durationBehind.getSeconds() % 60);
            info(LOG, ImmutableMap.of("channelName", channelName, "recordCount", recordCount, "durationBehind", durationString, "runtime", runtime), "Reading from stream", null);

        }
    }

}
