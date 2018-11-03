package de.otto.synapse.endpoint.receiver.kinesis;

import de.otto.synapse.message.Message;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static de.otto.synapse.channel.ShardPosition.*;
import static java.time.Instant.now;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static software.amazon.awssdk.services.kinesis.model.Record.builder;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.TRIM_HORIZON;

@RunWith(MockitoJUnitRunner.class)
public class KinesisShardIteratorTest {

    @Test
    public void shouldCreateShardIterator() {
        final KinesisShardIterator iterator = new KinesisShardIterator(someKinesisClient(), "", fromPosition("someShard", "42"));
        assertThat(iterator.getShardPosition(), is(fromPosition("someShard", "42")));
        assertThat(iterator.getId(), is("someShardIterator"));
        assertThat(iterator.getFetchRecordLimit(), is(10000));
    }

    @Test
    public void shouldCreateShardIteratorAtPosition() {
        final KinesisShardIterator iterator = new KinesisShardIterator(someKinesisClient(), "", atPosition("someShard", "42"));
        assertThat(iterator.getShardPosition(), is(atPosition("someShard", "42")));
        assertThat(iterator.getId(), is("someShardIterator"));
        assertThat(iterator.getFetchRecordLimit(), is(10000));
    }

    @Test
    public void shouldCreateShardIteratorWithFetchRecordLimit() {
        final KinesisShardIterator iterator = new KinesisShardIterator(someKinesisClient(), "", fromPosition("someShard", "42"), 1);
        assertThat(iterator.getShardPosition(), is(fromPosition("someShard", "42")));
        assertThat(iterator.getId(), is("someShardIterator"));
        assertThat(iterator.getFetchRecordLimit(), is(1));
    }

    @Test
    public void shouldFetchSingleMessage() {
        final Instant arrivalTimestamp = now();
        final Record record = Record.builder()
                .sequenceNumber("43")
                .approximateArrivalTimestamp(arrivalTimestamp)
                .partitionKey("someKey")
                .build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        final KinesisAsyncClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(completedFuture(response));

        final KinesisShardIterator iterator = new KinesisShardIterator(kinesisClient, "", fromPosition("someShard", "42"), 1);
        final KinesisShardResponse shardResponse = iterator.next();

        assertThat(shardResponse.getMessages(), hasSize(1));
        final Message<String> message = shardResponse.getMessages().get(0);
        assertThat(message.getKey(), is("someKey"));
        assertThat(message.getPayload(), is(nullValue()));
        assertThat(message.getHeader().getArrivalTimestamp(), is(arrivalTimestamp));
        assertThat(message.getHeader().getShardPosition().get(), is(fromPosition("someShard", "43")));
    }

    @Test
    public void shouldGetShardPosition() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(builder()
                        .sequenceNumber("someSeqNumber")
                        .partitionKey("1")
                        .approximateArrivalTimestamp(now())
                        .build())
                .millisBehindLatest(42L)
                .nextShardIterator("next")
                .build();
        final KinesisAsyncClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(completedFuture(response));
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromHorizon("someShard"));

        // when
        shardIterator.next();

        // then
        assertThat(shardIterator.getShardPosition(), is(fromPosition("someShard", "someSeqNumber")));
    }

    @Test
    public void shouldKeepShardPositionAfterEmptyResponse() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(emptyList())
                .millisBehindLatest(42L)
                .build();
        final KinesisAsyncClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(completedFuture(response));
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromPosition("someShard", "42"));

        // when
        shardIterator.next();

        // then
        assertThat(shardIterator.getShardPosition(), is(fromPosition("someShard", "42")));
    }

    @Test
    public void shouldReturnTrimHorizonShardIteratorWhenStartingAtHorizon() {

        // when
        final KinesisAsyncClient kinesisClient = someKinesisClient();
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
        KinesisAsyncClient kinesisClient = mock(KinesisAsyncClient.class);
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenAnswer((Answer< CompletableFuture<GetShardIteratorResponse>>) invocation -> {
                    if ("4711".equals(((GetShardIteratorRequest)invocation.getArgument(0)).startingSequenceNumber())) {
                        throw InvalidArgumentException.builder().message("Bumm!").build();
                    }
                    return completedFuture(GetShardIteratorResponse.builder()
                            .shardIterator("someShardIterator")
                            .build());
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
        final KinesisAsyncClient kinesisClient = someKinesisClient();
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
        final KinesisAsyncClient kinesisClient = someKinesisClient();
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
                .millisBehindLatest(42L)
                .records(builder()
                        .sequenceNumber("someSeqNumber")
                        .partitionKey("foo")
                        .approximateArrivalTimestamp(now())
                        .build())
                .build();
        final KinesisAsyncClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(completedFuture(response));
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromHorizon("someShard"));

        // when
        final KinesisShardResponse fetchedResponse = shardIterator.next();

        // then
        assertThat(fetchedResponse.getShardPosition(), is(fromPosition("someShard", "someSeqNumber")));
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
                .records(emptyList())
                .nextShardIterator("nextIteratorId")
                .millisBehindLatest(42L)
                .build();
        KinesisAsyncClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(completedFuture(response));
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
                .records(emptyList())
                .nextShardIterator("nextIteratorId")
                .millisBehindLatest(42L)
                .build();
        final KinesisAsyncClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class)))
                .thenThrow(KinesisException.builder().message("forced test exception").build())
                .thenThrow(KinesisException.builder().message("forced test exception").build())
                .thenThrow(KinesisException.builder().message("forced test exception").build())
                .thenThrow(KinesisException.builder().message("forced test exception").build())
                .thenReturn(completedFuture(response));
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromHorizon("someShard"));

        // when
        shardIterator.next();

        // then
        verify(kinesisClient, times(5)).getRecords(any(GetRecordsRequest.class));
        assertThat(shardIterator.getId(), is("nextIteratorId"));
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionWhenStoppingInRetry() {
        // given
        final KinesisAsyncClient kinesisClient = someKinesisClient();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class)))
                .thenReturn(supplyAsync(() -> {
                    throw KinesisException.builder().message("forced test exception").build();
                }));
        final KinesisShardIterator shardIterator = new KinesisShardIterator(kinesisClient, "", fromHorizon("someShard"));

        // when
        shardIterator.stop();
        shardIterator.next();

        // then throw exception
    }


    private static KinesisAsyncClient someKinesisClient() {
        final KinesisAsyncClient kinesisClient = mock(KinesisAsyncClient.class);
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(completedFuture(GetShardIteratorResponse
                .builder()
                .shardIterator("someShardIterator")
                .build()));
        return kinesisClient;
    }
}
