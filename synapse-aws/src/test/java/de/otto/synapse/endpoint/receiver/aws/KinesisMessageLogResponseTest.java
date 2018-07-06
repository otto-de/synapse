package de.otto.synapse.endpoint.receiver.aws;


import de.otto.synapse.channel.ChannelPosition;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.time.Instant;

import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisMessageLogResponseTest {

    private Instant now;

    @Test
    public void shouldCalculateChannelPosition() {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        when(recordsResponse.records()).thenReturn(emptyList());
        final KinesisMessageLogResponse response = new KinesisMessageLogResponse(
                asList(
                        new KinesisShardResponse("foo", fromHorizon("foo"), recordsResponse, 1000),
                        new KinesisShardResponse("foo", fromPosition("bar", "42"), recordsResponse, 1000)
                )
        );
        assertThat(response.getChannelPosition(), is(ChannelPosition.channelPosition(fromHorizon("foo"), fromPosition("bar", "42"))));
    }

    @Test
    public void shouldReturnMessages() {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        now = now();
        when(recordsResponse.records()).thenReturn(asList(
                Record.builder().partitionKey("a").sequenceNumber("1").approximateArrivalTimestamp(now).build(),
                Record.builder().partitionKey("b").sequenceNumber("2").approximateArrivalTimestamp(now).build()
        ));
        final KinesisMessageLogResponse response = new KinesisMessageLogResponse(
                singletonList(
                        new KinesisShardResponse("foo", fromHorizon("foo"), recordsResponse, 1000)
                )
        );
        assertThat(response.getMessages(), contains(
                message("a", responseHeader(fromPosition("foo", "1"), now), null),
                message("b", responseHeader(fromPosition("foo", "2"), now), null)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToCreateResponseWithoutAnyShardResponses() {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        when(recordsResponse.records()).thenReturn(emptyList());
        new KinesisMessageLogResponse(
                emptyList()
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToCreateResponseFromDifferentChannels() {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        when(recordsResponse.records()).thenReturn(emptyList());
        new KinesisMessageLogResponse(
                asList(
                        new KinesisShardResponse("foo", fromHorizon("foo"), recordsResponse, 1000),
                        new KinesisShardResponse("bar", fromHorizon("bar"), recordsResponse, 1000)
                )
        );
    }
}