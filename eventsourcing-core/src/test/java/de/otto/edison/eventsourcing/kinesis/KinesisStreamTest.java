package de.otto.edison.eventsourcing.kinesis;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

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
    public void shouldRetrieveAllOpenShards() throws Exception {
        // given
        DescribeStreamResponse response = DescribeStreamResponse.builder()
                .streamDescription(StreamDescription.builder()
                        .shards()
                        .hasMoreShards(false)
                        .build())
                .build();
        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(response);

        // when
        List<KinesisShard> shards = kinesisStream.retrieveAllOpenShards();

        // then
        assertThat(shards, hasSize(0));
    }
}
