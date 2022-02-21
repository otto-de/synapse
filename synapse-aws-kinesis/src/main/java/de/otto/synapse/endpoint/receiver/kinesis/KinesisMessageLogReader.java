package de.otto.synapse.endpoint.receiver.kinesis;

import com.google.common.annotations.VisibleForTesting;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.ShardResponse;
import org.slf4j.Logger;
import org.slf4j.Marker;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelResponse.channelResponse;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

public class KinesisMessageLogReader {

    private static final Logger LOG = getLogger(KinesisMessageLogReader.class);

    private final String channelName;
    private final KinesisAsyncClient kinesisClient;
    private final ExecutorService executorService;
    private final Clock clock;
    private final AtomicReference<List<KinesisShardReader>> kinesisShardReaders = new AtomicReference<>();

    public static final int SKIP_NEXT_PARTS = 8;
    public static final int DEFAULT_WAITING_TIME_ON_EMPTY_RECORDS = 10000;
    public static final int DEFAULT_WAITING_TIME_ON_SKIP_EMPTY_PARTS = 200; // max 5 calls per second per shard

    private final int waitingTimeOnEmptyRecords;
    private final int skipNextEmptyParts;
    private final int waitingTimeOnSkipEmptyParts;
    private final Marker marker;


    public KinesisMessageLogReader(final String channelName,
                                   final KinesisAsyncClient kinesisClient,
                                   final ExecutorService executorService,
                                   final Clock clock) {
        this(channelName, kinesisClient, executorService, clock, DEFAULT_WAITING_TIME_ON_EMPTY_RECORDS, null);
    }

    public KinesisMessageLogReader(final String channelName,
                                   final KinesisAsyncClient kinesisClient,
                                   final ExecutorService executorService,
                                   final Clock clock, final int waitingTimeOnEmptyRecords, final Marker marker) {
        this(channelName, kinesisClient, executorService, clock, waitingTimeOnEmptyRecords, SKIP_NEXT_PARTS, DEFAULT_WAITING_TIME_ON_SKIP_EMPTY_PARTS, marker);
    }

    public KinesisMessageLogReader(final String channelName,
                                   final KinesisAsyncClient kinesisClient,
                                   final ExecutorService executorService,
                                   final Clock clock,
                                   final int waitingTimeOnEmptyRecords,
                                   final int skipNextEmptyParts,
                                   final int waitingTimeOnSkipEmptyParts,
                                   final Marker marker) {
        this.channelName = channelName;
        this.kinesisClient = kinesisClient;
        this.executorService = executorService;
        this.clock = clock;

        this.waitingTimeOnEmptyRecords = waitingTimeOnEmptyRecords;
        this.skipNextEmptyParts = skipNextEmptyParts;
        this.waitingTimeOnSkipEmptyParts = waitingTimeOnSkipEmptyParts;

        this.marker = marker;
    }

    public String getChannelName() {
        return channelName;
    }

    public List<String> getOpenShards() {
        if (kinesisShardReaders.get() == null) {
            initShards();
        }
        return kinesisShardReaders.get().stream()
                .map(KinesisShardReader::getShardName)
                .collect(toList());
    }

    public KinesisMessageLogIterator getMessageLogIterator(final ChannelPosition channelPosition) {
        if (kinesisShardReaders.get() == null) {
            initShards();
        }
        try {
            final List<CompletableFuture<KinesisShardIterator>> futureShardPositions = kinesisShardReaders.get()
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
            stop();
            this.kinesisShardReaders.set(null);
            throw e;
        }
    }

    public CompletableFuture<ChannelResponse> read(final KinesisMessageLogIterator iterator) {
        if (kinesisShardReaders.get() == null) {
            initShards();
        }
        try {

            final List<CompletableFuture<ShardResponse>> futureShardPositions = kinesisShardReaders
                    .get()
                    .stream()
                    .map(shardReader -> supplyAsync(
                            () -> {
                                final KinesisShardIterator shardIterator = iterator.getShardIterator(shardReader.getShardName());
                                return fetchNext(shardIterator, skipNextEmptyParts);
                            },
                            executorService))
                    .collect(toList());
            return supplyAsync(() -> channelResponse(channelName, futureShardPositions
                    .stream()
                    .map(CompletableFuture::join)
                    .collect(toImmutableList())), executorService);
        } catch (final RuntimeException e) {
            stop();
            this.kinesisShardReaders.set(null);
            throw e;
        }
    }

    private ShardResponse fetchNext(final KinesisShardIterator shardIterator, int skipNextParts) {
        final String id = shardIterator.getId();
        final ShardResponse shardResponse = shardIterator.next();
        if(shardResponse.getMessages().isEmpty() && !shardIterator.isPoison() && !Objects.equals(shardIterator.getId(), id) && skipNextParts > 0) {
            try {
                Thread.sleep(waitingTimeOnSkipEmptyParts);  // avoid to many request in a short timespan
            } catch (InterruptedException e) {
                LOG.warn(marker, "Thread got interrupted");
            }
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
     */
    public CompletableFuture<ChannelPosition> consumeUntil(final ChannelPosition startFrom,
                                                           final Predicate<ShardResponse> stopCondition,
                                                           final Consumer<ShardResponse> consumer) {
        if (kinesisShardReaders.get() == null) {
            initShards();
        }
        try {
            final List<CompletableFuture<ShardPosition>> futureShardPositions = kinesisShardReaders
                    .get()
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
                stop();
                this.kinesisShardReaders.set(null);
                throw new RuntimeException(throwable.getMessage(), throwable);
            }));
        } catch (final RuntimeException e) {
            stop();
            this.kinesisShardReaders.set(null);
            throw e;
        }
    }

    private void initShards() {
        final Set<String> openShards = retrieveAllOpenShards();
        this.kinesisShardReaders.set(openShards
                .stream()
                .map(shardName -> new KinesisShardReader(channelName, shardName, kinesisClient, executorService, clock, waitingTimeOnEmptyRecords, marker))
                .collect(toList()));
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


    public void stop() {
        LOG.info("Channel {} received stop signal.", getChannelName());
        if (kinesisShardReaders.get() != null) {
            this.kinesisShardReaders.get().forEach(KinesisShardReader::stop);
        }
    }

    @VisibleForTesting
    List<KinesisShardReader> getCurrentKinesisShards() {
        if (kinesisShardReaders.get() == null) {
            initShards();
        }
        return kinesisShardReaders.get();
    }
}
