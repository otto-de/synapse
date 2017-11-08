package de.otto.edison.eventsourcing.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static java.lang.String.format;
import static java.time.Duration.ofMillis;

public class KinesisShard {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisShard.class);

    private final String shardId;
    private final KinesisClient kinesisClient;
    private final KinesisStream kinesisStream;

    public KinesisShard(String shardId, KinesisStream kinesisStream, KinesisClient kinesisClient) {
        this.shardId = shardId;
        this.kinesisStream = kinesisStream;
        this.kinesisClient = kinesisClient;
    }

    public String getShardId() {
        return shardId;
    }

    public KinesisShardIterator retrieveIterator(String sequenceNumber) {
        GetShardIteratorResponse shardIteratorResponse;
        try {
            shardIteratorResponse = kinesisClient.getShardIterator(buildIteratorShardRequest(sequenceNumber));
        } catch (final InvalidArgumentException e) {
            LOG.error(format("invalidShardSequenceNumber in Snapshot %s/%s - reading from HORIZON", kinesisStream.getStreamName(), shardId));
            shardIteratorResponse = kinesisClient.getShardIterator(buildIteratorShardRequest("0"));
        }
        return new KinesisShardIterator(kinesisClient, shardIteratorResponse.shardIterator());
    }

    private GetShardIteratorRequest buildIteratorShardRequest(String sequenceNumber) {
        GetShardIteratorRequest.Builder shardRequestBuilder = GetShardIteratorRequest
                .builder()
                .shardId(shardId)
                .streamName(kinesisStream.getStreamName());

        if (sequenceNumber == null || sequenceNumber.equals("0")) {
            shardRequestBuilder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        } else {
            shardRequestBuilder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            shardRequestBuilder.startingSequenceNumber(sequenceNumber);
        }

        return shardRequestBuilder.build();
    }

    public ShardPosition consumeRecordsAndReturnLastSeqNumber(String startFromSeqNumber,
                                                       BiFunction<Long, Record, Boolean> stopCondition,
                                                       BiConsumer<Long, Record> consumer) {
        LOG.info("Reading from stream {}, shard {} with starting sequence number {}",
                kinesisStream.getStreamName(),
                shardId,
                startFromSeqNumber);

        KinesisShardIterator shardIterator = retrieveIterator(startFromSeqNumber);
        String lastSequenceNumber = startFromSeqNumber;
        boolean stopRetrieval;
        do {
            GetRecordsResponse recordsResponse = shardIterator.next();

            stopRetrieval = stopCondition.apply(null, null);
            if (!isEmptyStream(recordsResponse)) {
                Long millisBehindLatest = recordsResponse.millisBehindLatest();
                for (final Record record : recordsResponse.records()) {
                    consumer.accept(millisBehindLatest, record);
                    stopRetrieval = stopCondition.apply(millisBehindLatest, record);
                    lastSequenceNumber = record.sequenceNumber();
                }

                logInfo(kinesisStream.getStreamName(), recordsResponse, ofMillis(millisBehindLatest));
            }
            if (!stopRetrieval) {
                stopRetrieval = waitABit();
            }
        } while (!stopRetrieval);
        LOG.info("Terminating event source for stream {}", kinesisStream.getStreamName());
        return new ShardPosition(shardId, lastSequenceNumber);
    }

    private void logInfo(String streamName, GetRecordsResponse recordsResponse, Duration durationBehind) {
        final String durationString = format("%s days %s hrs %s min %s sec", durationBehind.toDays(), durationBehind.toHours() % 24, durationBehind.toMinutes() % 60, durationBehind.getSeconds() % 60);
        LOG.info("Consumed {} records from kinesis {}; behind latest: {}",
                recordsResponse.records().size(),
                streamName,
                durationString);
    }

    private boolean waitABit() {
        try {
            /* See DECISIONS.md - Question #1 */
            Thread.sleep(1000);
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
