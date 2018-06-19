package de.otto.synapse.endpoint.receiver.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import de.otto.synapse.info.MessageReceiverStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static de.otto.synapse.channel.ChannelDurationBehind.copyOf;
import static de.otto.synapse.channel.ChannelDurationBehind.unknown;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.info.MessageReceiverStatus.*;
import static de.otto.synapse.logging.LogHelper.info;
import static java.lang.String.format;
import static java.time.Duration.ofMillis;
import static java.util.Objects.isNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toList;

public class KinesisMessageLogReceiverEndpoint extends AbstractMessageLogReceiverEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisMessageLogReceiverEndpoint.class);


    private final KinesisClient kinesisClient;
    private final Clock clock;
    private List<KinesisShardReader> kinesisShardReaders;
    private ExecutorService executorService;
    private final AtomicReference<ChannelDurationBehind> channelDurationBehind = new AtomicReference<>();

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
                    .map(KinesisShardReader::getShardId)
                    .collect(toList());
            channelDurationBehind.set(unknown(shards));

            publishEvent(STARTED, "Received shards from Kinesis.", null);

            final List<CompletableFuture<ShardPosition>> futureShardPositions = kinesisShardReaders
                    .stream()
                    .map(shard -> supplyAsync(
                            () -> consumeShard(shard, startFrom.shard(shard.getShardId()), until),
                            executorService))
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

    private ShardPosition consumeShard(final KinesisShardReader shard,
                                       final ShardPosition startFrom,
                                       final Instant until) {
        MDC.put("channelName", getChannelName());
        MDC.put("shardId", shard.getShardId());
        info(LOG, ImmutableMap.of("position", startFrom), "Reading from stream", null);
        try {

            return shard.consumeUntil(startFrom, until, getMessageDispatcher());

        } catch (final RuntimeException e) {
            LOG.error("Failed to consume from Kinesis shard {}: {}", getChannelName(), shard.getShardId(), e.getMessage());
            // Stop all shards and shutdown if this shard is failing:
            stop();
            throw e;
        } finally {
            MDC.remove("channelName");
            MDC.remove("shardId");
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
                .map(shard -> new KinesisShardReader(getChannelName(), shard.shardId(), kinesisClient, getInterceptorChain(), clock) {
                    private volatile long batchStarted;

                    @Override
                    public void beforeBatch() {
                        super.beforeBatch();
                        batchStarted = System.currentTimeMillis();
                    }

                    @Override
                    public void afterBatch(final GetRecordsResponse response) {
                        super.afterBatch(response);
                        final Duration shardBehindLatest = ofMillis(response.millisBehindLatest());
                        channelDurationBehind.updateAndGet(behind -> {
                            return copyOf(behind)
                                    .with(shard.shardId(), shardBehindLatest)
                                    .build();
                        });
                        final long batchFinished = System.currentTimeMillis();
                        publishEvent(MessageReceiverStatus.RUNNING, "Reading from kinesis shard.", channelDurationBehind.get());
                        logInfo(getChannelName(), response, shardBehindLatest, batchFinished - batchStarted);
                    }
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

    private void logInfo(String channelName, GetRecordsResponse recordsResponse, Duration durationBehind, long runtime) {
        int recordCount = recordsResponse.records().size();
        boolean isBehind = durationBehind.getSeconds() > 0;
        if (recordCount > 0 || isBehind) {
            final String durationString = format("%s days %s hrs %s min %s sec", durationBehind.toDays(), durationBehind.toHours() % 24, durationBehind.toMinutes() % 60, durationBehind.getSeconds() % 60);
            info(LOG, ImmutableMap.of("channelName", channelName, "recordCount", recordCount, "durationBehind", durationString, "runtime", runtime), "Reading from stream", null);

        }
    }

}
