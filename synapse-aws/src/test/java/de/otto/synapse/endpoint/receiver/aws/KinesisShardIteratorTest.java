package de.otto.synapse.endpoint.receiver.aws;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.time.Instant;

import static de.otto.synapse.channel.ShardPosition.*;
import static java.time.Instant.now;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.TRIM_HORIZON;

@RunWith(MockitoJUnitRunner.class)
public class KinesisShardIteratorTest {

    public KinesisClient someKinesisClient() {
        final KinesisClient kinesisClient = mock(KinesisClient.class);
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(GetShardIteratorResponse
                .builder()
                .shardIterator("someShardIterator")
                .build());
        return kinesisClient;
    }

    @Test
    public void shouldGetShardPosition() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(Record.builder()
                        .sequenceNumber("someSeqNumber")
                        .build())
                .build();
        final KinesisClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromHorizon("someShard"));

        // when
        GetRecordsResponse fetchedResponse = shardIterator.next();

        // then
        assertThat(shardIterator.getShardPosition(), is(fromPosition("someShard", "someSeqNumber")));
    }

    @Test
    public void shouldKeepShardPositionAfterEmptyResponse() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .build();
        final KinesisClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromPosition("someShard", "42"));

        // when
        GetRecordsResponse fetchedResponse = shardIterator.next();

        // then
        assertThat(shardIterator.getShardPosition(), is(fromPosition("someShard", "42")));
    }

    @Test
    public void shouldReturnTrimHorizonShardIteratorWhenStartingAtHorizon() {

        // when
        final KinesisClient kinesisClient = someKinesisClient();
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "someChannel", fromHorizon("someShard"));

        // then
        assertThat(shardIterator.getId(), is("someShardIterator"));

        GetShardIteratorRequest expectedRequest = GetShardIteratorRequest.builder()
                .streamName("someChannel")
                .shardId("someShard")
                .shardIteratorType(TRIM_HORIZON)
                .build();
        verify(kinesisClient).getShardIterator(expectedRequest);
    }

    @Test
    public void shouldReturnTrimHorizonShardIteratorWhenRetrieveIteratorFailsWithInvalidArgumentException() {
        // when
        KinesisClient kinesisClient = mock(KinesisClient.class);
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenAnswer((Answer<GetShardIteratorResponse>) invocation -> {
                    if ("4711".equals(((GetShardIteratorRequest)invocation.getArgument(0)).startingSequenceNumber())) {
                        throw InvalidArgumentException.builder().message("Bumm!").build();
                    }
                    return GetShardIteratorResponse.builder()
                            .shardIterator("someShardIterator")
                            .build();
                });

        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "someChannel", fromHorizon("someShard"));

        // then
        assertThat(shardIterator.getId(), is("someShardIterator"));

        GetShardIteratorRequest expectedRequest = GetShardIteratorRequest.builder()
                .streamName("someChannel")
                .shardId("someShard")
                .shardIteratorType(TRIM_HORIZON)
                .build();
        verify(kinesisClient).getShardIterator(expectedRequest);
    }

    @Test
    public void shouldReturnAfterSequenceNumberIterator() {
        // when
        final KinesisClient kinesisClient = someKinesisClient();
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "someChannel", fromPosition("someShard", "1"));

        // then
        assertThat(shardIterator.getId(), is("someShardIterator"));

        GetShardIteratorRequest expectedRequest = GetShardIteratorRequest.builder()
                .streamName("someChannel")
                .shardId("someShard")
                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .startingSequenceNumber("1")
                .build();
        verify(kinesisClient).getShardIterator(expectedRequest);
    }

    @Test
    public void shouldReturnAtTimestampIterator() {
        // when
        final Instant now = now();
        final KinesisClient kinesisClient = someKinesisClient();
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "someChannel", fromTimestamp("someShard", now));

        // then
        assertThat(shardIterator.getId(), is("someShardIterator"));

        GetShardIteratorRequest expectedRequest = GetShardIteratorRequest.builder()
                .streamName("someChannel")
                .shardId("someShard")
                .shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                .timestamp(now)
                .build();
        verify(kinesisClient).getShardIterator(expectedRequest);
    }
    @Test
    public void shouldFetchRecords() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(Record.builder()
                        .sequenceNumber("someSeqNumber")
                        .build())
                .build();
        final KinesisClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromHorizon("someShard"));

        // when
        GetRecordsResponse fetchedResponse = shardIterator.next();

        // then
        assertThat(fetchedResponse, is(response));
        GetRecordsRequest expectedRequest = GetRecordsRequest.builder()
                .shardIterator("someShardIterator")
                .limit(KinesisShardIterator.FETCH_RECORDS_LIMIT)
                .build();
        verify(kinesisClient).getRecords(expectedRequest);
    }

    @Test
    public void shouldIterateToNextId() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextIteratorId")
                .build();
        KinesisClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromHorizon("someShard"));

        // when
        shardIterator.next();

        // then
        assertThat(shardIterator.getId(), is("nextIteratorId"));
    }


    @Test
    public void shouldRetryReadingIteratorOnKinesisException() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextIteratorId")
                .build();
        final KinesisClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class)))
                .thenThrow(new KinesisException("forced test exception"))
                .thenThrow(new KinesisException("forced test exception"))
                .thenThrow(new KinesisException("forced test exception"))
                .thenThrow(new KinesisException("forced test exception"))
                .thenReturn(response);
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromHorizon("someShard"));

        // when
        shardIterator.next();

        // then
        verify(kinesisClient, times(5)).getRecords(any(GetRecordsRequest.class));
        assertThat(shardIterator.getId(), is("nextIteratorId"));

    }

    @Test
    public void shouldStopAfterSucessfulResponse() {
        // given
        GetRecordsResponse expectedResponse = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextIteratorId")
                .build();
        final KinesisClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(expectedResponse);
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromHorizon("someShard"));

        // when
        shardIterator.stop();
        GetRecordsResponse returnedResponse = shardIterator.next();
        // then
        assertThat(returnedResponse, is(expectedResponse));
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionWhenStoppingInRetry() {
        // given
        final KinesisClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class)))
                .thenThrow(new KinesisException("forced test exception"));
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromHorizon("someShard"));

        // when
        shardIterator.stop();
        shardIterator.next();

        // then throw exception
    }

}
