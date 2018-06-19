package de.otto.synapse.endpoint.receiver.aws;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.otto.synapse.message.aws.KinesisMessage.kinesisMessage;

@ThreadSafe
public class KinesisShardReader {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardReader.class);

    private final String shardId;
    private final String channelName;
    private final KinesisClient kinesisClient;
    private final InterceptorChain interceptorChain;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);
    private final Clock clock;

    public KinesisShardReader(final String channelName,
                              final String shardId,
                              final KinesisClient kinesisClient,
                              final InterceptorChain interceptorChain,
                              final Clock clock) {
        this.shardId = shardId;
        this.channelName = channelName;
        this.kinesisClient = kinesisClient;
        this.interceptorChain = interceptorChain;
        this.clock = clock;
    }

    public String getShardId() {
        return shardId;
    }

    public void beforeBatch() {
    }

    public void afterBatch(final GetRecordsResponse response) {
    }

    public final ShardPosition consume(final ShardPosition fromPosition,
                                       final MessageConsumer<String> consumer) {
        final KinesisShardIterator kinesisShardIterator = new KinesisShardIterator(kinesisClient, channelName, fromPosition);
        return consume(kinesisShardIterator, consumer);
    }

    public final ShardPosition consume(final ShardPosition fromPosition,
                                       final MessageConsumer<String> consumer,
                                       final int fetchRecordLimit) {
        final KinesisShardIterator kinesisShardIterator = new KinesisShardIterator(kinesisClient, channelName, fromPosition, fetchRecordLimit);

        return consume(kinesisShardIterator, consumer);
    }

    public final ShardPosition consume(final KinesisShardIterator kinesisShardIterator,
                                       final MessageConsumer<String> consumer) {
        beforeBatch();

        final GetRecordsResponse recordsResponse = kinesisShardIterator.next();
        for (final Record record : recordsResponse.records()) {
            final Message<String> message = interceptorChain.intercept(kinesisMessage(shardId, record));
            if (message != null) {
                consumeMessageSafely(message, consumer);
            }
        }

        afterBatch(recordsResponse);
        return kinesisShardIterator.getShardPosition();
    }

    public final ShardPosition consumeUntil(final ShardPosition fromPosition,
                                            final Instant until,
                                            final MessageConsumer<String> consumer) {
        final KinesisShardIterator kinesisShardIterator = new KinesisShardIterator(kinesisClient, channelName, fromPosition);
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
            consume(kinesisShardIterator, consumer);

            stopRetrieval = !until.isAfter(Instant.now(clock)) || stopSignal.get() || waitABit();

        } while (!stopRetrieval);
        return kinesisShardIterator.getShardPosition();
    }


    private void consumeMessageSafely(final Message<String> message,
                                      final MessageConsumer<String> consumer) {
        try {
            consumer.accept(message);
        } catch (Exception e) {
            LOG.error("consumer failed while processing {}", message, e);
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
        stopSignal.set(true);
    }

    public boolean isStopping() {
        return stopSignal.get();
    }

}
