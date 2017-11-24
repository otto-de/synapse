package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KinesisStreamTest {

    @Mock
    private KinesisClient kinesisClient;

    private TextEncryptor textEncryptor = Encryptors.noOpText();

    private KinesisStream kinesisStream;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        kinesisStream = new KinesisStream(kinesisClient, "streamName", objectMapper, textEncryptor);
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
        // when
        kinesisStream.sendEvent("someKey", new ExampleJsonObject("banana"));

        // then
        ArgumentCaptor<PutRecordRequest> captor = ArgumentCaptor.forClass(PutRecordRequest.class);
        verify(kinesisClient).putRecord(captor.capture());
        PutRecordRequest putRecordRequest = captor.getValue();

        assertThat(putRecordRequest.partitionKey(), is("someKey"));
        assertThat(putRecordRequest.streamName(), is("streamName"));

        ExampleJsonObject exampleJsonObject = objectMapper.readValue(new ByteBufferBackedInputStream(putRecordRequest.data()), ExampleJsonObject.class);
        assertThat(exampleJsonObject.value, is("banana"));
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


    public static class ExampleJsonObject {
        @JsonProperty
        private String value;

        public ExampleJsonObject() {
        }

        public ExampleJsonObject(String value) {
            this.value = value;
        }
    }
}
