package de.otto.synapse.endpoint.receiver.aws;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static de.otto.synapse.endpoint.receiver.aws.KinesisShardIterator.FETCH_RECORDS_LIMIT;
import static java.util.Optional.ofNullable;

@ThreadSafe
public class KinesisShardReader {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardReader.class);

    private final String shardName;
    private final String channelName;
    private final KinesisClient kinesisClient;
    private final Clock clock;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);

    public KinesisShardReader(final String channelName,
                              final String shardName,
                              final KinesisClient kinesisClient,
                              final Clock clock) {
        this.shardName = shardName;
        this.channelName = channelName;
        this.kinesisClient = kinesisClient;
        this.clock = clock;
    }

    public String getChannelName() {
        return channelName;
    }

    public String getShardName() {
        return shardName;
    }

    public KinesisShardIterator createIterator(final ShardPosition fromPosition) {
        return createIterator(fromPosition, FETCH_RECORDS_LIMIT);
    }

    public KinesisShardIterator createIterator(final ShardPosition fromPosition, final int fetchRecordLimit) {
        return new KinesisShardIterator(kinesisClient, channelName, fromPosition, fetchRecordLimit);
    }

    public final KinesisShardResponse read(final KinesisShardIterator kinesisShardIterator) {

        return kinesisShardIterator.next();
    }

    /**
     * Reads a single {@link Message} from a Kinesis shard.
     * <p>
     *     The Message is the next message after the specified {@code shardPosition}
     * </p>
     * <p>
     *     <em>Caution:</em> By calling this method, a KinesisShardIterator is created, which is an expensive operation.
     *     Creating a KinesisShardIterator too often may result in a {@link ProvisionedThroughputExceededException}
     *     coming from the Amazon Kinesis SDK as described
     *     {@link KinesisClient#getShardIterator(GetShardIteratorRequest) here}. If calling this method results in
     *     {@code ProvisionedThroughputExceededExceptions}, you should wait for some time (one second is recommended)
     *     before trying it again, as Amazon is only allowing 5(!) GetShardIteratorRequests per second.
     * </p>
     */
    @Nonnull
    public Optional<Message<String>> fetchOne(final ShardPosition shardPosition) {
        return ofNullable(createIterator(shardPosition, 1).next().getMessages().get(0));
    }

    public ShardPosition consumeUntil(final ShardPosition startFrom,
                                      final Instant until,
                                      final Consumer<KinesisShardResponse> responseConsumer) {

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

                final KinesisShardResponse response = kinesisShardIterator.next();
                responseConsumer.accept(response);

                stopRetrieval = !until.isAfter(Instant.now(clock)) || isStopping() || waitABit();

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
