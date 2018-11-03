package de.otto.synapse.endpoint.receiver.kinesis;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisStreamInfoProviderTest {

    private KinesisStreamInfoProvider testee;
    private KinesisAsyncClient kinesisAsyncClient;

    @Before
    public void setUp() {
        kinesisAsyncClient = mock(KinesisAsyncClient.class);
        testee = new KinesisStreamInfoProvider(kinesisAsyncClient);


    }

    @Test
    public void shouldReturnStreamInfo() {
        //given
        when(kinesisAsyncClient
                .describeStream(any(DescribeStreamRequest.class)))
                .thenReturn(completedFuture(DescribeStreamResponse.builder()
                        .streamDescription(StreamDescription.builder()
                                .streamName("someChannelName")
                                .streamARN("arn:aws:kinesis:eu-central-1:123456789012:stream/someChannelName")
                                .shards(someOpenShard("firstShard"))
                                .hasMoreShards(false)
                                .build())
                        .build()));

        //when
        KinesisStreamInfo kinesisStreamInfo = testee.getStreamInfo("someChannelName");

        //then
        assertThat(kinesisStreamInfo.getChannelName(), is("someChannelName"));
        assertThat(kinesisStreamInfo.getArn(), is("arn:aws:kinesis:eu-central-1:123456789012:stream/someChannelName"));
        assertThat(kinesisStreamInfo.getShardInfo(), is(ImmutableList.of(new KinesisShardInfo("firstShard", true))));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionForUnknownStream() {
        //given
        when(kinesisAsyncClient
                .describeStream(any(DescribeStreamRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        //when
        testee.getStreamInfo("someChannelName");
    }

    @Test(expected = SdkException.class)
    public void shouldThrowOtherExceptions() {
        //given
        when(kinesisAsyncClient
                .describeStream(any(DescribeStreamRequest.class)))
                .thenThrow(SdkException.class);

        //when
        testee.getStreamInfo("someChannelName");
    }

    @Test
    public void shouldReturnShardInfosFromMultipleBatches() {
        //given
        when(kinesisAsyncClient
                .describeStream(any(DescribeStreamRequest.class)))
                .thenReturn(
                        completedFuture(DescribeStreamResponse.builder()
                                .streamDescription(StreamDescription.builder()
                                        .streamName("someChannelName")
                                        .streamARN("arn:aws:kinesis:eu-central-1:123456789012:stream/someChannelName")
                                        .shards(someOpenShard("firstShard"))
                                        .hasMoreShards(true)
                                        .build())
                                .build()),
                        completedFuture(DescribeStreamResponse.builder()
                                .streamDescription(StreamDescription.builder()
                                        .streamName("someChannelName")
                                        .streamARN("arn:aws:kinesis:eu-central-1:123456789012:stream/someChannelName")
                                        .shards(someOpenShard("secondShard"))
                                        .hasMoreShards(false)
                                        .build())
                                .build()));

        //when
        KinesisStreamInfo kinesisStreamInfo = testee.getStreamInfo("someChannelName");

        //then
        assertThat(kinesisStreamInfo.getChannelName(), is("someChannelName"));
        assertThat(kinesisStreamInfo.getArn(), is("arn:aws:kinesis:eu-central-1:123456789012:stream/someChannelName"));
        assertThat(kinesisStreamInfo.getShardInfo(), is(ImmutableList.of(
                new KinesisShardInfo("firstShard", true),
                new KinesisShardInfo("secondShard", true))));
    }

    @Test
    public void shouldReturnClosedShardInfos() {
        //given
        when(kinesisAsyncClient
                .describeStream(any(DescribeStreamRequest.class)))
                .thenReturn(
                        completedFuture(DescribeStreamResponse.builder()
                                .streamDescription(StreamDescription.builder()
                                        .streamName("someChannelName")
                                        .streamARN("arn:aws:kinesis:eu-central-1:123456789012:stream/someChannelName")
                                        .shards(someClosedShard("firstShard"))
                                        .hasMoreShards(true)
                                        .build())
                                .build()),
                        completedFuture(DescribeStreamResponse.builder()
                                .streamDescription(StreamDescription.builder()
                                        .streamName("someChannelName")
                                        .streamARN("arn:aws:kinesis:eu-central-1:123456789012:stream/someChannelName")
                                        .shards(someOpenShard("secondShard"))
                                        .hasMoreShards(false)
                                        .build())
                                .build()));

        //when
        KinesisStreamInfo kinesisStreamInfo = testee.getStreamInfo("someChannelName");

        //then
        assertThat(kinesisStreamInfo.getChannelName(), is("someChannelName"));
        assertThat(kinesisStreamInfo.getArn(), is("arn:aws:kinesis:eu-central-1:123456789012:stream/someChannelName"));
        assertThat(kinesisStreamInfo.getShardInfo(), is(ImmutableList.of(
                new KinesisShardInfo("firstShard", false),
                new KinesisShardInfo("secondShard", true))));
    }

    private Shard someOpenShard(final String shardName) {
        return Shard.builder().shardId(shardName).sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("0").endingSequenceNumber(null).build()).build();
    }

    private Shard someClosedShard(final String shardName) {
        return Shard.builder().shardId(shardName).sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("0").endingSequenceNumber("42").build()).build();
    }
}