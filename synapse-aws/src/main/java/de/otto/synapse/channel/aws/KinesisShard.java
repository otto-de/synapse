package de.otto.synapse.channel.aws;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.time.Duration;
import java.util.function.Predicate;

import static de.otto.synapse.channel.Status.OK;
import static de.otto.synapse.channel.Status.STOPPED;
import static de.otto.synapse.message.aws.KinesisMessage.kinesisMessage;
import static java.lang.String.format;
import static java.time.Duration.ofMillis;

public class KinesisShard {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisShard.class);

    private final String shardId;
    private final String streamName;
    private final KinesisClient kinesisClient;

    public KinesisShard(final String shardId,
                        final String streamName,
                        final KinesisClient kinesisClient) {
        this.shardId = shardId;
        this.streamName = streamName;
        this.kinesisClient = kinesisClient;
    }

    public String getShardId() {
        return shardId;
    }

    public ChannelResponse consumeShard(final ChannelPosition channelPosition,
                                        final Predicate<Message<?>> stopCondition,
                                        final MessageConsumer<String> consumer) {
        String lastSequenceNumber = channelPosition.positionOf(shardId);
        boolean stopRetrieval = false;
        try {
            LOG.info("Reading from stream {}, shard {} with starting sequence number {}",
                    streamName,
                    shardId,
                    lastSequenceNumber);

            final GetRecordsResponse recordsResponse = retrieveIterator(lastSequenceNumber).next();
            final Duration durationBehind = ofMillis(recordsResponse.millisBehindLatest());

            if (!isEmptyStream(recordsResponse)) {
                for (final Record record : recordsResponse.records()) {
                    try {
                        final Message<String> kinesisMessage = kinesisMessage(durationBehind, record);
                        consumer.accept(kinesisMessage);
                        stopRetrieval = stopRetrieval || stopCondition.test(kinesisMessage);
                    } catch (Exception e) {
                        LOG.error("consumer failed while processing {}: {}", record, e);
                    }
                    lastSequenceNumber = record.sequenceNumber();
                }

                logInfo(streamName, recordsResponse, durationBehind);
            }
        } catch (final Exception e) {
            LOG.error("Kinesis consumer died unexpectedly.", e);
            throw e;
        }
        return ChannelResponse.of(
                stopRetrieval ? STOPPED : OK,
                ChannelPosition.of(shardId, lastSequenceNumber));
    }

    public KinesisShardIterator retrieveIterator(final String sequenceNumber) {
        GetShardIteratorResponse shardIteratorResponse;
        try {
            shardIteratorResponse = kinesisClient.getShardIterator(buildIteratorShardRequest(sequenceNumber));
        } catch (final InvalidArgumentException e) {
            LOG.error(format("invalidShardSequenceNumber in Snapshot %s/%s - reading from HORIZON", streamName, shardId));
            // TODO: "0" used as "magic value" to start from beginning.
            shardIteratorResponse = kinesisClient.getShardIterator(buildIteratorShardRequest("0"));
        }
        return new KinesisShardIterator(kinesisClient, shardIteratorResponse.shardIterator());
    }

    private GetShardIteratorRequest buildIteratorShardRequest(final String sequenceNumber) {
        final GetShardIteratorRequest.Builder shardRequestBuilder = GetShardIteratorRequest
                .builder()
                .shardId(shardId)
                .streamName(streamName);

        if (sequenceNumber == null || sequenceNumber.equals("0")) {
            shardRequestBuilder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        } else {
            shardRequestBuilder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            shardRequestBuilder.startingSequenceNumber(sequenceNumber);
        }

        return shardRequestBuilder.build();
    }


    private void logInfo(final String streamName,
                         final GetRecordsResponse recordsResponse,
                         final Duration durationBehind) {
        final String durationString = format("%s days %s hrs %s min %s sec", durationBehind.toDays(), durationBehind.toHours() % 24, durationBehind.toMinutes() % 60, durationBehind.getSeconds() % 60);
        LOG.info("Consumed {} records from stream '{}' shard '{}'; behind latest: {}",
                recordsResponse.records().size(),
                streamName,
                shardId,
                durationString);
    }

    private boolean isEmptyStream(final GetRecordsResponse recordsResponse) {
        return recordsResponse.records().isEmpty();
    }

}
