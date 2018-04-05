package de.otto.synapse.channel.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageLogReceiverEndpointTest {

    private static final Pattern MATCH_ALL = Pattern.compile(".*");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private KinesisClient kinesisClient;

    @Captor
    private ArgumentCaptor<Message<String>> messageArgumentCaptor;
    @Mock
    private MessageConsumer<String> messageConsumer;

    private KinesisMessageLogReceiverEndpoint kinesisMessageLog;
    private int nextKey = 0;

    @Before
    public void setUp() throws Exception {
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint(kinesisClient, objectMapper,"channelName");
        when(messageConsumer.keyPattern()).thenReturn(MATCH_ALL);
        when(messageConsumer.payloadType()).thenReturn(String.class);
    }

    @Test
    public void shouldRetrieveEmptyListOfShards() {
        // given
        describeStreamResponse(ImmutableList.of());

        // when
        List<KinesisShard> shards = kinesisMessageLog.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(0));
    }

    @Test
    public void shouldRetrieveSingleOpenShard() {
        // given
        describeStreamResponse(ImmutableList.of(someShard("shard1", true)));

        // when
        List<KinesisShard> shards = kinesisMessageLog.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(1));
        assertThat(shards.get(0).getShardId(), is("shard1"));
    }

    @Test
    public void shouldRetrieveOnlyOpenShards() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", false),
                        someShard("shard3", true)));

        // when
        List<KinesisShard> shards = kinesisMessageLog.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardId(), is("shard1"));
        assertThat(shards.get(1).getShardId(), is("shard3"));
    }

    @Test
    public void shouldRetrieveShardsOfMultipleResponses() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", true)),
                ImmutableList.of(
                        someShard("shard3", true),
                        someShard("shard4", true)));

        // when
        List<KinesisShard> shards = kinesisMessageLog.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(4));
        assertThat(shards.get(0).getShardId(), is("shard1"));
        assertThat(shards.get(1).getShardId(), is("shard2"));
        assertThat(shards.get(2).getShardId(), is("shard3"));
        assertThat(shards.get(3).getShardId(), is("shard4"));
    }

    @Test
    public void shouldRetrieveShardsOfMultipleResponsesWithFirstAllClosed() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", false),
                        someShard("shard2", false)),
                ImmutableList.of(
                        someShard("shard3", true),
                        someShard("shard4", true)));

        // when
        List<KinesisShard> shards = kinesisMessageLog.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardId(), is("shard3"));
        assertThat(shards.get(1).getShardId(), is("shard4"));
    }

    @Test
    public void shouldConsumeAllEventsFromKinesis() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", "iter1");

        KinesisMessageLogReceiverEndpoint kinesisMessageLog = new KinesisMessageLogReceiverEndpoint(kinesisClient, objectMapper, "testStream");
        kinesisMessageLog.register(messageConsumer);

        // when
        ChannelPosition finalChannelPosition = kinesisMessageLog.consume(ChannelPosition.fromHorizon(), this::stopIfGreenForString);

        // then
        verify(messageConsumer, times(3)).accept(messageArgumentCaptor.capture());
        List<Message<String>> messages = messageArgumentCaptor.getAllValues();

        assertThat(messages.get(0).getPayload(), is("{\"data\":\"blue\"}"));
        assertThat(messages.get(1).getPayload(), is(nullValue()));
        assertThat(messages.get(2).getPayload(), is("{\"data\":\"green\"}"));
        assertThat(finalChannelPosition.shard("shard1").position(), is("sequence-green"));
    }

    @Test
    public void shouldShutdownOnStop() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", "iter1");

        KinesisMessageLogReceiverEndpoint kinesisMessageLog = new KinesisMessageLogReceiverEndpoint(kinesisClient, objectMapper, "testStream");
        kinesisMessageLog.register(messageConsumer);

        // when
        kinesisMessageLog.stop();
        ChannelPosition finalChannelPosition = kinesisMessageLog.consume(ChannelPosition.fromHorizon(), (m) -> false);

        // then
        assertThat(finalChannelPosition, is(channelPosition(fromHorizon("shard1"))));
    }

    @Test(expected = RuntimeException.class)
    public void shouldShutdownOnException() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("failing-shard2", true))
        );
        describeRecordsForShard("shard1", "iter1");
        describeRecordsForShard("failing-shard2", "failing-iter2");

        KinesisMessageLogReceiverEndpoint kinesisMessageLog = new KinesisMessageLogReceiverEndpoint(kinesisClient, objectMapper, "testStream");
        kinesisMessageLog.register(messageConsumer);

        // when
        kinesisMessageLog.consume(ChannelPosition.fromHorizon(), (m) -> false);
    }

    private Shard someShard(String shardId, boolean open) {
        return Shard.builder()
                .shardId(shardId)
                .sequenceNumberRange(SequenceNumberRange.builder()
                        .startingSequenceNumber("0000")
                        .endingSequenceNumber(open ? null : "1111")
                        .build())
                .build();
    }

    private void describeStreamResponse(List<Shard> shards) {
        DescribeStreamResponse response = createResponseForShards(shards, false);

        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(response);
    }

    private void describeStreamResponse(List<Shard> firstShardBatch, List<Shard> secondShardBatch) {
        DescribeStreamResponse firstResponse = createResponseForShards(firstShardBatch, true);
        DescribeStreamResponse secondResponse = createResponseForShards(secondShardBatch, false);

        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(firstResponse, secondResponse);
    }

    private void describeRecordsForShard(String shardName, String iteratorName) {
        when(kinesisClient.getShardIterator(argThat((GetShardIteratorRequest req) -> req != null && req.shardId().equals
                (shardName))))
                .thenReturn(
                GetShardIteratorResponse.builder()
                .shardIterator(iteratorName)
                .build());

        GetRecordsResponse response0 = GetRecordsResponse.builder()
                .records()
                .millisBehindLatest(555L)
                .nextShardIterator("iterator1")
                .build();
        GetRecordsResponse response1 = GetRecordsResponse.builder()
                .records(
                        createRecord("blue"))
                .millisBehindLatest(1234L)
                .nextShardIterator("nextIterator")
                .build();
        GetRecordsResponse response2 = GetRecordsResponse.builder()
                .records(
                        createEmptyRecord(),
                        createRecord("green"))
                .millisBehindLatest(2345L)
                .nextShardIterator("yetAnotherIterator")
                .build();

        when(kinesisClient.getRecords(argThat((GetRecordsRequest req) -> req != null && req.shardIterator().contains("failing"))))
                .thenThrow(new RuntimeException("boo!"));

        when(kinesisClient.getRecords(argThat((GetRecordsRequest req) -> req == null || !req.shardIterator().contains("failing"))))
                .thenReturn(response0, response1, response2);
    }

    private DescribeStreamResponse createResponseForShards(List<Shard> shards, boolean hasMoreShards) {
        return DescribeStreamResponse.builder()
                .streamDescription(StreamDescription.builder()
                        .shards(shards)
                        .hasMoreShards(hasMoreShards)
                        .build())
                .build();
    }

    private Record createRecord(String data) {
        String json = "{\"data\":\"" + data + "\"}";
        return Record.builder()
                .partitionKey(String.valueOf(nextKey++))
                .approximateArrivalTimestamp(Instant.now())
                .data(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)))
                .sequenceNumber("sequence-" + data)
                .build();
    }

    private Record createEmptyRecord() {
        return Record.builder()
                .partitionKey(String.valueOf(nextKey++))
                .approximateArrivalTimestamp(Instant.now())
                .data(ByteBuffer.allocateDirect(0))
                .sequenceNumber("sequence-" + "empty")
                .build();
    }


    private boolean stopIfGreenForString(Message<?> message) {
        return message.getPayload() != null && message.getPayload().toString().contains("green");
    }

}
