package de.otto.edison.eventsourcing.kinesis;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class KinesisStream {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStream.class);
    static final int PUT_RECORDS_BATCH_SIZE = 500;

    private final KinesisClient kinesisClient;
    private final String streamName;
    private final RetryPutRecordsKinesisClient retryPutRecordsKinesisClient;

    public KinesisStream(KinesisClient kinesisClient, String streamName) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        retryPutRecordsKinesisClient = new RetryPutRecordsKinesisClient(kinesisClient);
    }

    public List<KinesisShard> retrieveAllOpenShards() {
        List<Shard> shardList = retrieveAllShards();

        return shardList.stream()
                .filter(this::isShardOpen)
                .map(shard -> new KinesisShard(shard.shardId(), this, kinesisClient))
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

    public String getStreamName() {
        return streamName;
    }

    public void send(String key, ByteBuffer byteBuffer) {
        PutRecordsRequestEntry putRecordsRequestEntry = requestEntryFor(key, byteBuffer);

        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder()
                .streamName(streamName)
                .records(putRecordsRequestEntry)
                .build();

        retryPutRecordsKinesisClient.putRecords(putRecordsRequest);
    }

    public void sendMultiple(Map<String, ByteBuffer> eventMap) {
        ArrayList<PutRecordsRequestEntry> entries = eventMap.entrySet().stream()
                .map(entry -> requestEntryFor(entry.getKey(), entry.getValue()))
                .collect(Collectors.toCollection(ArrayList::new));

        Lists.partition(entries, PUT_RECORDS_BATCH_SIZE)
                .forEach(batch -> {
                            PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder()
                                    .streamName(streamName)
                                    .records(batch)
                                    .build();

                            retryPutRecordsKinesisClient.putRecords(putRecordsRequest);
                        }
                );

    }

    private PutRecordsRequestEntry requestEntryFor(String key, ByteBuffer byteBuffer) {
        return PutRecordsRequestEntry.builder()
                .partitionKey(key)
                .data(byteBuffer)
                .build();
    }
}
