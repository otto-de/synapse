package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class KinesisStream {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStream.class);

    private final KinesisClient kinesisClient;
    private final String streamName;
    private final ObjectMapper objectMapper;
    private final TextEncryptor textEncryptor;

    public KinesisStream(KinesisClient kinesisClient, String streamName,
                         ObjectMapper objectMapper, TextEncryptor textEncryptor) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.objectMapper = objectMapper;
        this.textEncryptor = textEncryptor;
    }

    public List<KinesisShard> retrieveAllOpenShards() {
        List<Shard> shardList = retrieveAllShards();

        return shardList.stream()
                .filter(this::isShardOpen)
                .map(shard -> new KinesisShard(shard.shardId(), this, kinesisClient))
                .collect(toImmutableList());
    }

    public <T> void sendEvent(String key, T payload) throws JsonProcessingException {
        String jsonData = objectMapper.writeValueAsString(payload);
        PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(key)
                .data(convertToEncryptedByteBuffer(jsonData))
                .build();
        kinesisClient.putRecord(putRecordRequest);
    }

    private ByteBuffer convertToEncryptedByteBuffer(String data) {
        return ByteBuffer.wrap(textEncryptor
                .encrypt(data)
                .getBytes(Charsets.UTF_8));
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
}
