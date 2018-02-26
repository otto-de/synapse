package de.otto.edison.eventsourcing.aws.kinesis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.otto.edison.eventsourcing.consumer.MessageConsumer;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.message.Message;
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
import java.util.List;

import static de.otto.edison.eventsourcing.aws.kinesis.Status.OK;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KinesisStreamTest {

    @Mock
    private KinesisClient kinesisClient;

    @Captor
    private ArgumentCaptor<Message<String>> messageArgumentCaptor;
    @Mock
    private MessageConsumer<String> messageConsumer;

    private KinesisStream kinesisStream;
    private int nextKey = 0;

    @Before
    public void setUp() throws Exception {
        kinesisStream = new KinesisStream(kinesisClient, "streamName");
    }

    @Test
    public void shouldRetrieveEmptyListOfShards() throws Exception {
        // given
        describeStreamResponse(ImmutableList.of());

        // when
        List<KinesisShard> shards = kinesisStream.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(0));
    }

    @Test
    public void shouldRetrieveSingleOpenShard() throws Exception {
        // given
        describeStreamResponse(ImmutableList.of(someShard("shard1", true)));

        // when
        List<KinesisShard> shards = kinesisStream.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(1));
        assertThat(shards.get(0).getShardId(), is("shard1"));
    }

    @Test
    public void shouldRetrieveOnlyOpenShards() throws Exception {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", false),
                        someShard("shard3", true)));

        // when
        List<KinesisShard> shards = kinesisStream.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardId(), is("shard1"));
        assertThat(shards.get(1).getShardId(), is("shard3"));
    }

    @Test
    public void shouldRetrieveShardsOfMultipleResponses() throws Exception {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", true)),
                ImmutableList.of(
                        someShard("shard3", true),
                        someShard("shard4", true)));

        // when
        List<KinesisShard> shards = kinesisStream.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(4));
        assertThat(shards.get(0).getShardId(), is("shard1"));
        assertThat(shards.get(1).getShardId(), is("shard2"));
        assertThat(shards.get(2).getShardId(), is("shard3"));
        assertThat(shards.get(3).getShardId(), is("shard4"));
    }

    @Test
    public void shouldRetrieveShardsOfMultipleResponsesWithFirstAllClosed() throws Exception {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", false),
                        someShard("shard2", false)),
                ImmutableList.of(
                        someShard("shard3", true),
                        someShard("shard4", true)));

        // when
        List<KinesisShard> shards = kinesisStream.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardId(), is("shard3"));
        assertThat(shards.get(1).getShardId(), is("shard4"));
    }

    @Test
    public void shouldConsumeAllEventsFromKinesis() {
        // given
        StreamPosition initialPositions = StreamPosition.of(ImmutableMap.of("shard1", "xyz"));

        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard();

        KinesisStream kinesisStream= new KinesisStream(kinesisClient, "testStream");

        // when
        StreamResponse response = StreamResponse.of(OK, initialPositions);
        do {
            response = kinesisStream.consumeStream(response.getStreamPosition(), this::stopIfGreenForString, messageConsumer);
        } while(response.getStatus() != Status.STOPPED);

        // then
        verify(messageConsumer, times(3)).accept(messageArgumentCaptor.capture());
        List<Message<String>> messages = messageArgumentCaptor.getAllValues();

        assertThat(messages.get(0).getPayload(), is("{\"data\":\"blue\"}"));
        assertThat(messages.get(1).getPayload(), is(nullValue()));
        assertThat(messages.get(2).getPayload(), is("{\"data\":\"green\"}"));
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

    private void describeRecordsForShard() {
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(GetShardIteratorResponse.builder()
                .shardIterator("someIterator")
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
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response0, response1, response2);
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
                .data(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)))
                .sequenceNumber("sequence-" + data)
                .build();
    }

    private Record createEmptyRecord() {
        return Record.builder()
                .partitionKey(String.valueOf(nextKey++))
                .data(ByteBuffer.allocateDirect(0))
                .sequenceNumber("sequence-" + "empty")
                .build();
    }


    private boolean stopIfGreenForString(Message<?> message) {
        if (message.getPayload() == null) {
            return false;
        }
        return message.getPayload().toString().contains("green");
    }

}
