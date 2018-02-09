package de.otto.edison.eventsourcing.aws.kinesis;

import com.google.common.collect.ImmutableList;
import de.otto.edison.eventsourcing.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static de.otto.edison.eventsourcing.message.Message.message;
import static java.lang.String.valueOf;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KinesisStreamTest {

    @Mock
    private KinesisClient kinesisClient;


    private KinesisStream kinesisStream;

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
    public void shouldSendEvent() throws Exception {
        // given
        PutRecordsResponse putRecordsResponse = PutRecordsResponse.builder()
                .failedRecordCount(0)
                .build();
        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(putRecordsResponse);

        ByteBuffer data = ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8));

        // when
        kinesisStream.send("someKey", data);

        // then
        ArgumentCaptor<PutRecordsRequest> captor = ArgumentCaptor.forClass(PutRecordsRequest.class);
        verify(kinesisClient).putRecords(captor.capture());
        PutRecordsRequest putRecordsRequest = captor.getValue();

        assertThat(putRecordsRequest.streamName(), is("streamName"));

        List<PutRecordsRequestEntry> records = putRecordsRequest.records();
        assertThat(records, hasSize(1));
        assertThat(records.get(0).partitionKey(), is("someKey"));
        assertThat(records.get(0).data(), is(data));
    }

    @Test
    public void shouldSendMultipleEvents() throws Exception {
        // given
        PutRecordsResponse putRecordsResponse = PutRecordsResponse.builder()
                .failedRecordCount(0)
                .build();
        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(putRecordsResponse);

        ByteBuffer data1 = ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8));
        ByteBuffer data2 = ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8));

        // when
        kinesisStream.sendBatch(Stream.of(
                message("event1", data1),
                message("event2", data2)));

        // then
        ArgumentCaptor<PutRecordsRequest> captor = ArgumentCaptor.forClass(PutRecordsRequest.class);
        verify(kinesisClient).putRecords(captor.capture());
        PutRecordsRequest putRecordsRequest = captor.getValue();

        assertThat(putRecordsRequest.streamName(), is("streamName"));

        List<PutRecordsRequestEntry> records = putRecordsRequest.records();
        assertThat(records, hasSize(2));
        assertThat(records.get(0).partitionKey(), is("event1"));
        assertThat(records.get(0).data(), is(data1));
        assertThat(records.get(1).partitionKey(), is("event2"));
        assertThat(records.get(1).data(), is(data2));
    }

    @Test
    public void shouldBatchEventsWhenTooManyShouldBeSent() throws Exception {
        // given
        PutRecordsResponse putRecordsResponse = PutRecordsResponse.builder()
                .failedRecordCount(0)
                .build();
        when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(putRecordsResponse);
        
        // when
        kinesisStream.sendBatch(someEvents(KinesisStream.PUT_RECORDS_BATCH_SIZE + 1));

        // then
        verify(kinesisClient, times(2)).putRecords(any(PutRecordsRequest.class));
    }

    private Stream<Message<ByteBuffer>> someEvents(int n) {
        return IntStream.range(0, n)
                .mapToObj(i -> message(valueOf(i), ByteBuffer.wrap(Integer.toString(i).getBytes(StandardCharsets.UTF_8))));
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

    private DescribeStreamResponse createResponseForShards(List<Shard> shards, boolean hasMoreShards) {
        return DescribeStreamResponse.builder()
                .streamDescription(StreamDescription.builder()
                        .shards(shards)
                        .hasMoreShards(hasMoreShards)
                        .build())
                .build();
    }


}
