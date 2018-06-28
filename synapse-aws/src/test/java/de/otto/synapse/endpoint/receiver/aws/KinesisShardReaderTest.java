package de.otto.synapse.endpoint.receiver.aws;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.Message;
import de.otto.synapse.testsupport.TestClock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.StartFrom.HORIZON;
import static java.time.Duration.ofMillis;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KinesisShardReaderTest {

    @Mock
    private KinesisClient kinesisClient;

    @Mock
    private Consumer<KinesisShardResponse> consumer;

    private Clock clock = TestClock.now();
    private KinesisShardReader kinesisShardReader;

    @Before
    public void setUp() {
        kinesisShardReader = new KinesisShardReader("someChannel", "someShard", kinesisClient, clock);

        GetShardIteratorResponse fakeResponse = GetShardIteratorResponse.builder()
                .shardIterator("someShardIterator")
                .build();

        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(fakeResponse);
    }

    @Test
    public void shouldCreateShardIterator() {
        final KinesisShardIterator iterator = kinesisShardReader.createIterator(fromPosition("someShard", "42"));
        assertThat(iterator.getShardPosition(), is(fromPosition("someShard", "42")));
        assertThat(iterator.getId(), is("someShardIterator"));
        assertThat(iterator.getFetchRecordLimit(), is(10000));
    }

    @Test
    public void shouldCreateShardIteratorWithFetchRecordLimit() {
        final KinesisShardIterator iterator = kinesisShardReader.createIterator(fromPosition("someShard", "42"), 1);
        assertThat(iterator.getShardPosition(), is(fromPosition("someShard", "42")));
        assertThat(iterator.getId(), is("someShardIterator"));
        assertThat(iterator.getFetchRecordLimit(), is(1));
    }

    @Test
    public void shouldFetchSingleMessage() {
        final Record record = Record.builder()
                .sequenceNumber("43")
                .approximateArrivalTimestamp(clock.instant().minus(1, HOURS))
                .partitionKey("someKey")
                .build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        final Message<String> message = kinesisShardReader
                .fetchOne(fromPosition("someShard", "42"))
                .orElse(null);

        assertThat(message.getKey(), is("someKey"));
        assertThat(message.getPayload(), is(nullValue()));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now(clock).minus(1, HOURS)));
        assertThat(message.getHeader().getShardPosition().get(), is(fromPosition("someShard", "43")));
    }

    @Test
    public void shouldReadFromIterator() {
        final Record record1 = Record.builder()
                .sequenceNumber("43")
                .approximateArrivalTimestamp(clock.instant().minus(42, HOURS))
                .partitionKey("someKey")
                .build();
        final Record record2 = Record.builder()
                .sequenceNumber("44")
                .approximateArrivalTimestamp(clock.instant().minus(43, HOURS))
                .partitionKey("someOtherKey")
                .build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record1, record2)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        final KinesisShardIterator iterator = kinesisShardReader.createIterator(fromPosition("someShard", "42"));

        final KinesisShardResponse shardResponse = kinesisShardReader.read(iterator);

        assertThat(shardResponse.getChannelName(), is("someChannel"));
        assertThat(shardResponse.getShardName(), is("someShard"));
        assertThat(shardResponse.getDurationBehind(), is(ofMillis(1234L)));
        assertThat(shardResponse.getShardPosition(), is(fromPosition("someShard", "44")));
        assertThat(shardResponse.getMessages(), hasSize(2));
        Message<String> message = shardResponse.getMessages().get(0);
        assertThat(message.getKey(), is("someKey"));
        assertThat(message.getPayload(), is(nullValue()));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now(clock).minus(42, HOURS)));
        assertThat(message.getHeader().getShardPosition().get(), is(fromPosition("someShard", "43")));
        message = shardResponse.getMessages().get(1);
        assertThat(message.getKey(), is("someOtherKey"));
        assertThat(message.getPayload(), is(nullValue()));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now(clock).minus(43, HOURS)));
        assertThat(message.getHeader().getShardPosition().get(), is(fromPosition("someShard", "44")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldConsumeSingleRecordSet() {
        final Instant now = now();
        final Instant future = now.plus(1, SECONDS);
        // given
        final Record record1 = Record.builder()
                .sequenceNumber("1")
                .approximateArrivalTimestamp(future)
                .partitionKey("first")
                .build();
        final Record record2 = Record.builder()
                .sequenceNumber("2")
                .approximateArrivalTimestamp(future)
                .partitionKey("second")
                .build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record1, record2)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        kinesisShardReader.stop();
        final ShardPosition shardPosition = kinesisShardReader.consumeUntil(fromHorizon("someShard"), now, consumer);

        // then
        verify(consumer).accept(new KinesisShardResponse(
                "someChannel",
                "someShard",
                fromPosition("someShard", "2"),
                response, 0
        ));
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.position(), is("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldConsumeEmptyRecordSet() {
        final Instant now = now();
        final Instant future = now.plus(1, SECONDS);
        // given
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        ArgumentCaptor<KinesisShardResponse> argumentCaptor = ArgumentCaptor.forClass(KinesisShardResponse.class);
        final ShardPosition shardPosition = kinesisShardReader.consumeUntil(fromHorizon("someShard"), now.minus(1, SECONDS), consumer);

        // then
        verify(consumer).accept(argumentCaptor.capture());
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.startFrom(), is(HORIZON));
        final KinesisShardResponse shardResponse = argumentCaptor.getValue();
        assertThat(shardResponse.getChannelName(), is("someChannel"));
        assertThat(shardResponse.getShardName(), is("someShard"));
        assertThat(shardResponse.getShardPosition(), is(fromHorizon("someShard")));
        assertThat(shardResponse.getDurationBehind(), is(ofMillis(1234L)));
        assertThat(shardResponse.getMessages(), hasSize(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldConsumeSingleRecordSetForStoppingThread() throws ExecutionException, InterruptedException, TimeoutException {
        final Instant now = now();
        final Instant future = now.plus(1, SECONDS);

        // given
        final Record record1 = Record.builder()
                .sequenceNumber("1")
                .approximateArrivalTimestamp(future)
                .partitionKey("first")
                .build();
        final Record record2 = Record.builder()
                .sequenceNumber("2")
                .approximateArrivalTimestamp(future)
                .partitionKey("second")
                .build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record1, record2)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        kinesisShardReader.stop();
        ArgumentCaptor<KinesisShardResponse> argumentCaptor = ArgumentCaptor.forClass(KinesisShardResponse.class);
        final CompletableFuture<ShardPosition> shardPosition = supplyAsync(() -> kinesisShardReader.consumeUntil(fromHorizon("someShard"), now, consumer));

        shardPosition.get();

        // then
        verify(consumer).accept(argumentCaptor.capture());
        verifyNoMoreInteractions(consumer);
        assertThat(kinesisShardReader.isStopping(), is(true));
        assertThat(shardPosition.get(), is(fromPosition("someShard", "2")));
        final KinesisShardResponse shardResponse = argumentCaptor.getValue();
        assertThat(shardResponse.getChannelName(), is("someChannel"));
        assertThat(shardResponse.getShardName(), is("someShard"));
        assertThat(shardResponse.getShardPosition(), is(fromPosition("someShard", "2")));
        assertThat(shardResponse.getDurationBehind(), is(ofMillis(1234L)));
        assertThat(shardResponse.getMessages(), hasSize(2));
        assertThat(shardResponse.getMessages().get(0).getKey(), is("first"));
        assertThat(shardResponse.getMessages().get(1).getKey(), is("second"));
    }

    @Test
    public void shouldReturnPositionWhenThereAreNoRecords() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        kinesisShardReader.stop();
        ShardPosition shardPosition = kinesisShardReader.consumeUntil(fromPosition("someShard", "42"), now(), consumer);

        // then
        verify(consumer).accept(any(KinesisShardResponse.class));
        assertThat(shardPosition.position(), is("42"));
    }

    @Test(expected = RuntimeException.class)
    public void shouldPropagateException() {
        // given
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenThrow(RuntimeException.class);

        // when
        kinesisShardReader.consumeUntil(fromHorizon("someShard"), Instant.MAX, consumer);

        // then
        // exception is thrown
    }
}
