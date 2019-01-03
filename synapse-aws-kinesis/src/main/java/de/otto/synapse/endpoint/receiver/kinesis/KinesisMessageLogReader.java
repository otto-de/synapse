package de.otto.synapse.endpoint.receiver.kinesis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.ShardResponse;
import org.slf4j.Logger;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelResponse.channelResponse;
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
    private final KinesisAsyncClient kinesisClient;
    private final Clock clock;
    private List<KinesisShardReader> kinesisShardReaders;
    private ExecutorService executorService;

    public static final int SKIP_NEXT_PARTS = 8;

    public KinesisMessageLogReader(final String channelName,
                                   final KinesisAsyncClient kinesisClient,
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

    public CompletableFuture<ChannelResponse> read(final KinesisMessageLogIterator iterator) {
        if (isNull(executorService)) {
            initExecutorService();
        }
        try {

            final List<CompletableFuture<ShardResponse>> futureShardPositions = kinesisShardReaders
                    .stream()
                    .map(shardReader -> supplyAsync(
                            () -> {
                                final KinesisShardIterator shardIterator = iterator.getShardIterator(shardReader.getShardName());
                                return fetchNext(shardIterator, SKIP_NEXT_PARTS);
                            },
                            executorService))
                    .collect(toList());
            return supplyAsync(() -> channelResponse(channelName, futureShardPositions
                    .stream()
                    .map(CompletableFuture::join)
                    .collect(toImmutableList())), executorService);
        } catch (final RuntimeException e) {
            shutdownExecutor();
            throw e;
        }
    }

    private ShardResponse fetchNext(final KinesisShardIterator shardIterator, int skipNextParts) {
        final String id = shardIterator.getId();
        final ShardResponse shardResponse = shardIterator.next();
        if(shardResponse.getMessages().isEmpty() && !shardIterator.isPoison() && !Objects.equals(shardIterator.getId(), id) && skipNextParts > 0) {
            return fetchNext(shardIterator, --skipNextParts);
        }
        return shardResponse;
    }

    /**
     *
     * @param startFrom starting position
     * @param stopCondition stop condition used to stop message consumption
     * @param consumer the consumer used to process the {@link ShardResponse shard responses}
     * @return completable future
     * @deprecated to be removed soon
     */
    public CompletableFuture<ChannelPosition> consumeUntil(final ChannelPosition startFrom,
                                                           final Predicate<ShardResponse> stopCondition,
                                                           final Consumer<ShardResponse> consumer) {
        if (isNull(executorService)) {
            initExecutorService();
        }
        try {
            final List<CompletableFuture<ShardPosition>> futureShardPositions = kinesisShardReaders
                    .stream()
                    .map(shard -> shard.consumeUntil(startFrom.shard(shard.getShardName()), stopCondition, consumer))
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
        return new KinesisStreamInfoProvider(kinesisClient)
                .getStreamInfo(channelName)
                .getShardInfo()
                .stream()
                .filter(KinesisShardInfo::isOpen)
                .map(KinesisShardInfo::getShardName)
                .collect(toImmutableSet());
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
