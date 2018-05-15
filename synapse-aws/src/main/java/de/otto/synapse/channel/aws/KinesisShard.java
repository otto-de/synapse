package de.otto.synapse.channel.aws;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.logging.LogHelper.error;
import static de.otto.synapse.logging.LogHelper.info;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static de.otto.synapse.message.aws.KinesisMessage.kinesisMessage;
import static java.lang.String.format;
import static java.time.Duration.ofMillis;
import static java.time.Instant.now;

@ThreadSafe
public class KinesisShard {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisShard.class);

    private final String shardId;
    private final String channelName;
    private final KinesisClient kinesisClient;
    private final InterceptorChain interceptorChain;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);

    public KinesisShard(final String shardId,
                        final String channelName,
                        final KinesisClient kinesisClient,
                        final InterceptorChain interceptorChain) {
        this.shardId = shardId;
        this.channelName = channelName;
        this.kinesisClient = kinesisClient;
        this.interceptorChain = interceptorChain;
    }

    public String getShardId() {
        return shardId;
    }

    //TODO Refactor: Return a ShardPosition, not a ChannelPosition
    public ChannelPosition consumeShard(final ChannelPosition startPosition,
                                        final Predicate<Message<?>> stopCondition,
                                        final MessageConsumer<String> consumer) {
        try {
            MDC.put("channelName", channelName);
            MDC.put("shardId", shardId);
            info(LOG, ImmutableMap.of("position", startPosition), "Reading from stream", null);

            ShardPosition shardPosition = startPosition.shard(shardId);
            KinesisShardIterator kinesisShardIterator = retrieveIterator(shardPosition);
            boolean stopRetrieval = false;
            Record lastRecord = null;
            final long t0 = System.currentTimeMillis();
            do {
                GetRecordsResponse recordsResponse = kinesisShardIterator.next();
                Duration durationBehind = ofMillis(recordsResponse.millisBehindLatest());
                final long t1 = System.currentTimeMillis();
                if (!isEmptyStream(recordsResponse)) {
                    for (final Record record : recordsResponse.records()) {
                        final Message<String> message = interceptorChain.intercept(kinesisMessage(shardId, durationBehind, record));
                        if (message != null) {
                            consumeMessageSafely(consumer, record, message);

                            lastRecord = record;
                            shardPosition = fromPosition(shardId, record.sequenceNumber());

                            //consume all records of current iterator, even if stop condition is true
                            // because durationBehind is only per iterator, not per record and we want to consume all
                            // records
                            if (!stopRetrieval) {
                                stopRetrieval = stopCondition.test(message);
                            }
                        }
                    }
                } else {
                    Message<String> kinesisMessage = lastRecord != null
                            ? kinesisMessage(shardId, durationBehind, lastRecord)
                            : dirtyHackToStopThreadMessage(durationBehind);
                    stopRetrieval = stopCondition.test(kinesisMessage);
                }

                final long t2 = System.currentTimeMillis();
                logInfo(channelName, recordsResponse, durationBehind, t2-t1);

                stopRetrieval = stopRetrieval || stopSignal.get() || waitABit();

            } while (!stopRetrieval);
            final long t3 = System.currentTimeMillis();
            info(LOG, ImmutableMap.of( "position", lastRecord != null ? lastRecord.sequenceNumber() : "", "runtime", (t3-t0)), "Done consuming from shard.", null);
            return channelPosition(shardPosition);
        } catch (final Exception e) {
            error(LOG, ImmutableMap.of("channelName", channelName, "shardId", shardId),"kinesis consumer died unexpectedly", e);
            throw e;
        } finally {
            MDC.remove("channelName");
            MDC.remove("shardId");
        }
    }

    private Message<String> dirtyHackToStopThreadMessage(final Duration durationBehind) {
        return message(
                "no_key",
                responseHeader(fromHorizon(shardId), now(), durationBehind),
                null);
    }

    @VisibleForTesting
    protected KinesisShardIterator retrieveIterator(ShardPosition shardPosition) {
        GetShardIteratorResponse shardIteratorResponse;
        try {
            shardIteratorResponse = kinesisClient.getShardIterator(buildIteratorShardRequest(shardPosition));
        } catch (final InvalidArgumentException e) {
            LOG.error(format("invalidShardSequenceNumber in Snapshot %s/%s - reading from HORIZON", channelName, shardId));
            shardIteratorResponse = kinesisClient.getShardIterator(buildIteratorShardRequest(fromHorizon(shardId)));
        }
        return new KinesisShardIterator(kinesisClient, shardIteratorResponse.shardIterator());
    }

    private GetShardIteratorRequest buildIteratorShardRequest(ShardPosition shardPosition) {
        GetShardIteratorRequest.Builder shardRequestBuilder = GetShardIteratorRequest
                .builder()
                .shardId(shardId)
                .streamName(channelName);

        if (shardPosition == null || shardPosition.startFrom() == StartFrom.HORIZON) {
            shardRequestBuilder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        } else {
            shardRequestBuilder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            shardRequestBuilder.startingSequenceNumber(shardPosition.position());
        }

        return shardRequestBuilder.build();
    }

    private void consumeMessageSafely(MessageConsumer<String> consumer, Record record, Message<String> kinesisMessage) {
        try {
            consumer.accept(kinesisMessage);
        } catch (Exception e) {
            LOG.error("consumer failed while processing {}", record, e);
        }
    }

    private void logInfo(String channelName, GetRecordsResponse recordsResponse, Duration durationBehind, long runtime) {
        int recordCount = recordsResponse.records().size();
        boolean isBehind = durationBehind.getSeconds() > 0;
        if (recordCount > 0 || isBehind) {
            final String durationString = format("%s days %s hrs %s min %s sec", durationBehind.toDays(), durationBehind.toHours() % 24, durationBehind.toMinutes() % 60, durationBehind.getSeconds() % 60);
            info(LOG, ImmutableMap.of("recordCount", recordCount, "durationBehind", durationBehind, "runtime", runtime), "Reading from stream", null);

        }
    }

    private boolean waitABit() {
        try {
            /* See DECISIONS.md - Question #1 */
            Thread.sleep(500);
        } catch (InterruptedException e) {
            LOG.warn("Thread got interrupted");
            return true;
        }
        return false;
    }

    private boolean isEmptyStream(final GetRecordsResponse recordsResponse) {
        return recordsResponse.records().isEmpty();
    }

    public void stop() {
        stopSignal.set(true);
    }

    public boolean isStopping() {
        return stopSignal.get();
    }
}
