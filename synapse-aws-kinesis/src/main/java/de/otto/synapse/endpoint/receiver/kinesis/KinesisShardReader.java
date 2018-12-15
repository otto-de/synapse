package de.otto.synapse.endpoint.receiver.kinesis;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.ShardResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @deprecated to be removed soon
 */
@ThreadSafe
public class KinesisShardReader {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardReader.class);

    private final String shardName;
    private final String channelName;
    private final KinesisAsyncClient kinesisClient;
    private final ExecutorService executorService;
    private final Clock clock;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);

    public KinesisShardReader(final String channelName,
                              final String shardName,
                              final KinesisAsyncClient kinesisClient,
                              final ExecutorService executorService,
                              final Clock clock) {
        this.shardName = shardName;
        this.channelName = channelName;
        this.kinesisClient = kinesisClient;
        this.executorService = executorService;
        this.clock = clock;
    }

    public String getChannelName() {
        return channelName;
    }

    public String getShardName() {
        return shardName;
    }

    public CompletableFuture<ShardPosition> consumeUntil(final ShardPosition startFrom,
                                                         final Instant until,
                                                         final Consumer<ShardResponse> responseConsumer) {
        return consumeUntil(startFrom,  r -> !until.isAfter(Instant.now(clock)), responseConsumer);
    }

    public CompletableFuture<ShardPosition> catchUp(final ShardPosition startFrom,
                                                    final Consumer<ShardResponse> responseConsumer) {
        return consumeUntil(startFrom,  r -> Duration.ZERO.equals(r.getDurationBehind()), responseConsumer);
    }

    private CompletableFuture<ShardPosition> consumeUntil(final ShardPosition startFrom,
                                                         final Predicate<ShardResponse> stopCondition,
                                                         final Consumer<ShardResponse> responseConsumer) {
        return CompletableFuture.supplyAsync(() -> {
            MDC.put("channelName", channelName);
            MDC.put("shardName", shardName);
            LOG.info("Reading from channel={}, shard={}, position={}", channelName, shardName, startFrom);
            try {
                final KinesisShardIterator kinesisShardIterator = new KinesisShardIterator(kinesisClient, channelName, startFrom);
                boolean stopRetrieval;
                do {
                    /*
                    Poison-Pill injected by a test. This is helpful, if you want to write tests that should terminate
                    after a number of iterated shards.
                     */
                    if (kinesisShardIterator.isPoison()) {
                        LOG.warn("Received Poison-Pill - This should only happen during tests!");
                        break;
                    }

                    final ShardResponse response = kinesisShardIterator.next();
                    responseConsumer.accept(response);

                    stopRetrieval = stopCondition.test(response) || isStopping() || waitABit();

                } while (!stopRetrieval);
                return kinesisShardIterator.getShardPosition();

            } catch (final RuntimeException e) {
                LOG.error("Failed to consume from Kinesis shard {}: {}", channelName, shardName, e.getMessage());
                // Stop all shards and shutdown if this shard is failing:
                stop();
                throw e;
            } finally {
                MDC.remove("channelName");
                MDC.remove("shardName");
            }
        }, executorService);
    }

    private boolean waitABit() {
        try {
            /*Wait one second as documented by amazon: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html*/
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOG.warn("Thread got interrupted");
            return true;
        }
        return false;
    }

    public void stop() {
        LOG.info("Shard {} received stop signal.", shardName);
        stopSignal.set(true);
    }

    public boolean isStopping() {
        return stopSignal.get();
    }
}
