package de.otto.synapse.channel.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.info.MessageEndpointStatus;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.merge;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.logging.LogHelper.info;
import static java.util.Objects.isNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toList;

public class KinesisMessageLogReceiverEndpoint extends MessageLogReceiverEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisMessageLogReceiverEndpoint.class);


    private final KinesisClient kinesisClient;
    private final Clock clock;
    private List<KinesisShard> kinesisShards;
    private ExecutorService executorService;

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
            final long t1 = System.currentTimeMillis();
            if (isNull(executorService)) {
               initExecutorService();
            }
            final ChannelPosition currentChannelPosition = merge(
                    channelPosition(kinesisShards.stream().map(shard -> fromHorizon(shard.getShardId())).collect(toList())),
                    startFrom);
            publishEvent(currentChannelPosition, MessageEndpointStatus.STARTED, "Received shards from Kinesis.");

            final List<CompletableFuture<ShardPosition>> futureShardPositions = kinesisShards
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
            return channelPosition(shardPositions);
        } catch (final RuntimeException e) {
            LOG.error("Failed to consume from Kinesis stream {}: {}", getChannelName(), e.getMessage());
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
        this.kinesisShards = retrieveAllOpenShards();
        if (kinesisShards.isEmpty()) {
            this.executorService = newSingleThreadExecutor();
        } else {
            this.executorService = newFixedThreadPool(kinesisShards.size(),
                    new ThreadFactoryBuilder().setNameFormat("kinesis-message-log-%d").build());
        }
    }

    private ShardPosition consumeShard(final KinesisShard shard,
                                       final ShardPosition startFrom,
                                       final Instant until) {
        try {

            // TODO: ShardPosition zur ChannelPosition mergen??

            final Consumer<ShardPosition> publishNotificationCallback = shardPosition -> publishEvent(channelPosition(shardPosition), MessageEndpointStatus.RUNNING, "Reading from kinesis shard.");
            return shard.consumeShard(startFrom, until, getMessageDispatcher(), publishNotificationCallback);
        } catch (final RuntimeException e) {
            LOG.error("Failed to consume from Kinesis shard {}: {}", getChannelName(), shard.getShardId(), e.getMessage());
            // Stop all shards and shutdown if this shard is failing:
            stop();
            throw e;
        }
    }

    @Override
    public void stop() {
        this.kinesisShards.forEach(KinesisShard::stop);
    }

    @VisibleForTesting
    List<KinesisShard> getCurrentKinesisShards() {
        return kinesisShards;
    }

    private List<KinesisShard> retrieveAllOpenShards() {
        return retrieveAllShards().stream()
                .filter(this::isShardOpen)
                .map(shard -> new KinesisShard(shard.shardId(), getChannelName(), kinesisClient, getInterceptorChain(), clock))
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

}
