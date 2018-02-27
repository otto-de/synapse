package de.otto.synapse.aws.kinesis;

import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.StreamPosition;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.min;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;

//TODO: KinesisMessageLogReaderEndpoint?

public class KinesisMessageLog implements MessageLog {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisMessageLog.class);
    private static final int MAX_NUMBER_OF_THREADS = 10;

    private final String streamName;
    private final KinesisClient kinesisClient;

    public KinesisMessageLog(final KinesisClient kinesisClient,
                             final String streamName) {
        this.streamName = streamName;
        this.kinesisClient = kinesisClient;
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public StreamResponse consumeStream(final StreamPosition startFrom,
                                        final Predicate<Message<?>> stopCondition,
                                        final MessageConsumer<String> consumer) {
        final List<KinesisShard> kinesisShards = retrieveAllOpenShards();
        final ExecutorService executorService = newFixedThreadPool(min(kinesisShards.size(), MAX_NUMBER_OF_THREADS));
        try {
            final List<CompletableFuture<ShardResponse>> futureShardPositions = kinesisShards
                    .stream()
                    .map(shard -> supplyAsync(
                            () -> shard.consumeShard(startFrom.positionOf(shard.getShardId()), stopCondition, consumer),
                            executorService))
                    .collect(toList());

            // don't chain futureShardPositions with CompletableFuture::join as lazy execution will prevent threads from
            // running in parallel

            return StreamResponse.of(
                    futureShardPositions
                            .stream()
                            .map(CompletableFuture::join)
                            .collect(toList())
            );
        } catch (final RuntimeException e) {
            LOG.error("Failed to consume from Kinesis stream {}: {}", streamName, e.getMessage());
            // When an exception occurs in a completable future's thread, other threads continue running.
            // Stop all before proceeding.
            executorService.shutdownNow();
            try {
                boolean allThreadsSafelyTerminated = executorService.awaitTermination(30, TimeUnit.SECONDS);
                if (!allThreadsSafelyTerminated) {
                    LOG.error("Kinesis Thread for stream {} is still running", streamName);
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            throw e;
        }
    }

    List<KinesisShard> retrieveAllOpenShards() {
        List<Shard> shardList = retrieveAllShards();

        return shardList.stream()
                .filter(this::isShardOpen)
                .map(shard -> new KinesisShard(shard.shardId(), streamName, kinesisClient))
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
                .streamName(streamName)
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
