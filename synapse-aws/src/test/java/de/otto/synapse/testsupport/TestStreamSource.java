package de.otto.synapse.testsupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import de.otto.synapse.channel.ChannelPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;

/**
 * This class is a data source for supplying input to the Amazon Kinesis stream. It reads lines from the
 * input file specified in the constructor and emits them by calling String.getBytes() into the
 * stream defined in the KinesisConnectorConfiguration.
 */
public class TestStreamSource {

    private static final Logger LOG = LoggerFactory.getLogger(TestStreamSource.class);

    private final String channelName;
    private final KinesisClient kinesisClient;
    private final String inputFile;
    private final ObjectMapper objectMapper;

    private final Map<String, String> mapShardIdToFirstWrittenSequence = new HashMap<>();
    private final Map<String, String> mapShardIdToLastWrittenSequence = new HashMap<>();

    /**
     * Creates a new TestStreamSource.
     *
     * @param inputFile File containing record data to emit on each line
     */
    public TestStreamSource(KinesisClient kinesisClient, String channelName, String inputFile) {
        this.kinesisClient = kinesisClient;
        this.inputFile = inputFile;
        this.objectMapper = new ObjectMapper();
        this.channelName = channelName;
    }

    public void writeToStream() {
        mapShardIdToFirstWrittenSequence.clear();
        mapShardIdToLastWrittenSequence.clear();

        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(inputFile);
        if (inputStream == null) {
            throw new IllegalStateException("Could not find input file: " + inputFile);
        }
        try {
            putRecords(channelName, createFakeRecords());
            processInputStream(channelName, inputStream);
        } catch (IOException e) {
            LOG.error("Encountered exception while putting data in source stream.", e);
        }
    }

    public ChannelPosition getLastStreamPosition() {
        return channelPosition(mapShardIdToLastWrittenSequence
                .keySet()
                .stream()
                .map(shardId -> fromPosition(shardId, mapShardIdToLastWrittenSequence.get(shardId)))
                .collect(toImmutableList()));
    }

    public ChannelPosition getFirstReadPosition() {
        return channelPosition(mapShardIdToFirstWrittenSequence
                .keySet()
                .stream()
                .map(shardId -> fromPosition(shardId, mapShardIdToFirstWrittenSequence.get(shardId)))
                .collect(toImmutableList()));

    }

    /**
     * Process the input file and send PutRecordRequests to Amazon Kinesis.
     * <p>
     * This function serves to Isolate TestStreamSource logic so subclasses
     * can process input files differently.
     *
     * @param inputStream the input stream to process
     * @throws IOException throw exception if error processing inputStream.
     */
    protected void processInputStream(String channelName, InputStream inputStream) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            int lines = 0;
            List<PutRecordsRequestEntry> records = new ArrayList<>(100);

            while ((line = br.readLine()) != null) {
                KinesisMessageModel kinesisMessageModel = objectMapper.readValue(line, KinesisMessageModel.class);
                records.add(PutRecordsRequestEntry.builder()
                        .data(ByteBuffer.wrap(line.getBytes()))
                        .partitionKey(Integer.toString(kinesisMessageModel.getUserid()))
                        .build());
                lines++;
                if (lines % 100 == 0) {
                    putRecords(channelName, records);
                    LOG.info("writing data; lines: {}", lines);
                    records.clear();
                }
            }

            if (!records.isEmpty()) {
                putRecords(channelName, records);
            }
            LOG.info("Added {} records to stream source.", lines);
        }
    }

    private List<PutRecordsRequestEntry> createFakeRecords() {
        Map<String, String> hashKeysForShards = getStartHashKeysForShards(channelName);
        return hashKeysForShards.entrySet().stream()
                .map(entry -> PutRecordsRequestEntry.builder()
                        .data(ByteBuffer.wrap("{}".getBytes(Charsets.UTF_8)))
                        .partitionKey(entry.getValue())
                        .explicitHashKey(entry.getValue())
                        .build())
                .collect(toImmutableList());
    }

    public void putRecords(String channelName, List<PutRecordsRequestEntry> records) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("records must not be unknown");
        }

        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder()
                .streamName(channelName)
                .records(records)
                .build();
        List<PutRecordsResultEntry> writtenRecords = kinesisClient.putRecords(putRecordsRequest).records();
        collectFirstAndLastSequenceNumber(writtenRecords);
    }

    private void collectFirstAndLastSequenceNumber(List<PutRecordsResultEntry> writtenRecords) {
        writtenRecords.forEach(entry -> {
            mapShardIdToFirstWrittenSequence.putIfAbsent(entry.shardId(), entry.sequenceNumber());
            mapShardIdToLastWrittenSequence.put(entry.shardId(), entry.sequenceNumber());
        });
    }

    private Map<String, String> getStartHashKeysForShards(String channelName) {
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(channelName)
                .build();
        try {
            return kinesisClient.describeStream(describeStreamRequest).streamDescription()
                    .shards()
                    .stream()
                    .filter(this::isShardOpen)
                    .collect(Collectors.toMap(Shard::shardId, shard -> shard.hashKeyRange().startingHashKey()));
        } catch (SdkClientException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isShardOpen(Shard shard) {
        return shard.sequenceNumberRange().endingSequenceNumber() == null;
    }
}
