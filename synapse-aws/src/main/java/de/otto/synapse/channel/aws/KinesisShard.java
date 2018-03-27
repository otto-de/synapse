package de.otto.synapse.channel.aws;

import de.otto.synapse.channel.*;
import com.google.common.annotations.VisibleForTesting;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.Status.OK;
import static de.otto.synapse.channel.Status.STOPPED;
import static de.otto.synapse.message.aws.KinesisMessage.kinesisMessage;
import static java.lang.String.format;
import static java.time.Duration.ofMillis;

public class KinesisShard {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisShard.class);

    private final String shardId;
    private final String channelName;
    private final KinesisClient kinesisClient;

    public KinesisShard(final String shardId,
                        final String channelName,
                        final KinesisClient kinesisClient) {
        this.shardId = shardId;
        this.channelName = channelName;
        this.kinesisClient = kinesisClient;
    }

    public String getShardId() {
        return shardId;
    }

    //TODO Refactor: Return a ShardPosition, not a ChannelPosition
    public ChannelPosition consumeShard(final ChannelPosition startPosition,
                                        final Predicate<Message<?>> stopCondition,
                                        final MessageConsumer<String> consumer) {
        try {
            LOG.info("Reading from stream {}, shard {} with starting sequence number {}",
                    channelName,
                    shardId,
                    startPosition.shard(shardId));

            ShardPosition shardPosition = startPosition.shard(shardId);
            KinesisShardIterator kinesisShardIterator = retrieveIterator(shardPosition);
            boolean stopRetrieval = false;
            Record lastRecord = null;
            do {
                GetRecordsResponse recordsResponse = kinesisShardIterator.next();
                Duration durationBehind = ofMillis(recordsResponse.millisBehindLatest());
                if (!isEmptyStream(recordsResponse)) {
                    for (final Record record : recordsResponse.records()) {
                        Message<String> kinesisMessage = kinesisMessage(shardId, durationBehind, record);
                        consumeMessageSafely(consumer, record, kinesisMessage);

                        lastRecord = record;
                        shardPosition = fromPosition(shardId, record.sequenceNumber());

                        //consume all records of current iterator, even if stop condition is true
                        // because durationBehind is only per iterator, not per record and we want to consume all
                        // records
                        if (!stopRetrieval) {
                            stopRetrieval = stopCondition.test(kinesisMessage);
                        }
                    }
                } else {
                    Message kinesisMessage = lastRecord != null
                            ? kinesisMessage(shardId, durationBehind, lastRecord)
                            : dirtyHackToStopThreadMessage(durationBehind);
                    stopRetrieval = stopCondition.test(kinesisMessage);
                }

                logInfo(channelName, recordsResponse, durationBehind);

                if (!stopRetrieval) {
                    stopRetrieval = waitABit();
                }
            } while (!stopRetrieval);
            LOG.info("Done consuming from shard '{}' of stream '{}'.", channelName, shardId);
            return channelPosition(shardPosition);
        } catch (final Exception e) {
            LOG.error(String.format("kinesis consumer died unexpectedly. shard '%s', stream '%s'", channelName, shardId), e);
            throw e;
        }
    }

    private Message<Header> dirtyHackToStopThreadMessage(final Duration durationBehind) {
        return Message.message("no_key", Header.responseHeader(ShardPosition.fromHorizon(shardId), Instant.now(), durationBehind));
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

    private void logInfo(String channelName, GetRecordsResponse recordsResponse, Duration durationBehind) {
        final String durationString = format("%s days %s hrs %s min %s sec", durationBehind.toDays(), durationBehind.toHours() % 24, durationBehind.toMinutes() % 60, durationBehind.getSeconds() % 60);
        LOG.info("Got {} records from stream '{}' and shard '{}'; behind latest: {}",
                recordsResponse.records().size(),
                channelName,
                shardId,
                durationString);
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

    private boolean isEmptyStream(GetRecordsResponse recordsResponse) {
        return recordsResponse.records().isEmpty();
    }

}
