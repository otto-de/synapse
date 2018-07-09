package de.otto.synapse.endpoint.receiver.aws;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import org.slf4j.Logger;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static java.util.Objects.isNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

public class KinesisMessageLogReader {

    private static final Logger LOG = getLogger(KinesisMessageLogReader.class);

    private final String channelName;
    private final KinesisClient kinesisClient;
    private final Clock clock;
    private List<KinesisShardReader> kinesisShardReaders;
    private ExecutorService executorService;

    public KinesisMessageLogReader(final String channelName,
                                   final KinesisClient kinesisClient,
                                   final Clock clock) {
        this.channelName = channelName;
        this.kinesisClient = kinesisClient;
        this.clock = clock;
    }

    public String getChannelName() {
        return channelName;
    }

    public List<String> getOpenShards() {
        if (isNull(executorService)) {
            initExecutorService();
        }
        return kinesisShardReaders.stream()
                .map(KinesisShardReader::getShardName)
                .collect(toList());
    }

    public KinesisMessageLogIterator getMessageLogIterator(final ChannelPosition channelPosition) {
        if (isNull(executorService)) {
            initExecutorService();
        }
        try {
            final List<CompletableFuture<KinesisShardIterator>> futureShardPositions = kinesisShardReaders
                    .stream()
                    .map(shardReader -> supplyAsync(
                            () -> new KinesisShardIterator(kinesisClient, channelName, channelPosition.shard(shardReader.getShardName())),
                            executorService))
                    .collect(toList());
            return new KinesisMessageLogIterator(futureShardPositions
                    .stream()
                    .map(CompletableFuture::join)
                    .collect(toList()));
        } catch (final RuntimeException e) {
            shutdownExecutor();
            throw e;
        }
    }

    public CompletableFuture<KinesisMessageLogResponse> read(final KinesisMessageLogIterator iterator) {
        if (isNull(executorService)) {
            initExecutorService();
        }
        try {

            final List<CompletableFuture<KinesisShardResponse>> futureShardPositions = kinesisShardReaders
                    .stream()
                    .map(shardReader -> supplyAsync(
                            () -> {
                                final KinesisShardIterator shardIterator = iterator.getShardIterator(shardReader.getShardName());
                                return shardIterator.next();
                            },
                            executorService))
                    .collect(toList());
            return supplyAsync(() -> new KinesisMessageLogResponse(futureShardPositions
                    .stream()
                    .map(CompletableFuture::join)
                    .collect(toList())), executorService);
        } catch (final RuntimeException e) {
            shutdownExecutor();
            throw e;
        }
    }

    public CompletableFuture<ChannelPosition> consumeUntil(final ChannelPosition startFrom,
                                                           final Instant until,
                                                           final Consumer<KinesisShardResponse> consumer) {
        if (isNull(executorService)) {
            initExecutorService();
        }
        try {
            final List<CompletableFuture<ShardPosition>> futureShardPositions = kinesisShardReaders
                    .stream()
                    .map(shard -> shard.consumeUntil(startFrom.shard(shard.getShardName()), until, consumer))
                    .collect(toList());
            // don't chain futureShardPositions with CompletableFuture::join as lazy execution will prevent threads from
            // running in parallel
            return supplyAsync(() -> channelPosition(futureShardPositions
                    .stream()
                    .map(CompletableFuture::join)
                    .collect(toList()))
            ).exceptionally((throwable -> {
                shutdownExecutor();
                throw new RuntimeException(throwable.getMessage(), throwable);
            }));
        } catch (final RuntimeException e) {
            shutdownExecutor();
            throw e;
        }
    }

    private void initExecutorService() {
        final Set<String> openShards = retrieveAllOpenShards();
        if (openShards.isEmpty()) {
            this.executorService = newSingleThreadExecutor();
        } else {
            this.executorService = newFixedThreadPool(
                    openShards.size() + 1,
                    new ThreadFactoryBuilder().setNameFormat("kinesis-message-log-%d").build()
            );
        }
        this.kinesisShardReaders = openShards
                .stream()
                .map(shardName -> new KinesisShardReader(channelName, shardName, kinesisClient, executorService, clock))
                .collect(toList());
    }

    private Set<String> retrieveAllOpenShards() {
        return retrieveAllShards()
                .stream()
                .filter(this::isShardOpen)
                .map(Shard::shardId)
                .collect(toImmutableSet());
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

    private void shutdownExecutor() {
        if (executorService != null) {
            executorService.shutdownNow();
            try {
                boolean allThreadsSafelyTerminated = executorService.awaitTermination(30, SECONDS);
                if (!allThreadsSafelyTerminated) {
                    LOG.error("Kinesis Thread for stream {} is still running", getChannelName());
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            executorService = null;
        }
    }

    public void stop() {
        LOG.info("Channel {} received stop signal.", getChannelName());
        this.kinesisShardReaders.forEach(KinesisShardReader::stop);
    }

    @VisibleForTesting
    List<KinesisShardReader> getCurrentKinesisShards() {
        if (isNull(executorService)) {
            initExecutorService();
        }
        return kinesisShardReaders;
    }
}
