package de.otto.synapse.endpoint.receiver.aws;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;

import static de.otto.synapse.channel.ShardPosition.*;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static de.otto.synapse.message.aws.KinesisMessage.kinesisMessage;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.TRIM_HORIZON;

@RunWith(MockitoJUnitRunner.class)
public class KinesisShardTest {

    @Mock
    private KinesisClient kinesisClient;

    @Mock
    private MessageConsumer<String> consumer;

    private InterceptorChain interceptorChain;
    private KinesisShard kinesisShard;

    @Before
    public void setUp() {
        interceptorChain = new InterceptorChain();
        kinesisShard = new KinesisShard("someShard", "someStream", kinesisClient, interceptorChain, Clock.systemDefaultZone());

        GetShardIteratorResponse fakeResponse = GetShardIteratorResponse.builder()
                .shardIterator("someShardIterator")
                .build();

        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(fakeResponse);
    }

    @Test
    public void shouldReturnTrimHorizonShardIteratorWhenStartingAtHorizon() {
        // when
        KinesisShardIterator iterator = kinesisShard.retrieveIterator(fromHorizon("someShard"));

        // then
        assertThat(iterator.getId(), is("someShardIterator"));

        GetShardIteratorRequest expectedRequest = GetShardIteratorRequest.builder()
                .streamName("someStream")
                .shardId("someShard")
                .shardIteratorType(TRIM_HORIZON)
                .build();
        verify(kinesisClient).getShardIterator(expectedRequest);
    }

    @Test
    public void shouldReturnTrimHorizonShardIteratorWhenRetrieveIteratorFailsWithInvalidArgumentException() {
        // when
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenAnswer((Answer<GetShardIteratorResponse>) invocation -> {
                    if ("4711".equals(((GetShardIteratorRequest)invocation.getArgument(0)).startingSequenceNumber())) {
                        throw InvalidArgumentException.builder().message("Bumm!").build();
                    }
                    return GetShardIteratorResponse.builder()
                            .shardIterator("someShardIterator")
                            .build();
                });

        KinesisShardIterator iterator = kinesisShard.retrieveIterator(fromPosition("someShard", "4711"));

        // then
        assertThat(iterator.getId(), is("someShardIterator"));

        GetShardIteratorRequest expectedRequest = GetShardIteratorRequest.builder()
                .streamName("someStream")
                .shardId("someShard")
                .shardIteratorType(TRIM_HORIZON)
                .build();
        verify(kinesisClient).getShardIterator(expectedRequest);
    }

    @Test
    public void shouldReturnAfterSequenceNumberIterator() {
        // when
        KinesisShardIterator iterator = kinesisShard.retrieveIterator(fromPosition("someShard", "1"));

        // then
        assertThat(iterator.getId(), is("someShardIterator"));

        GetShardIteratorRequest expectedRequest = GetShardIteratorRequest.builder()
                .streamName("someStream")
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
        KinesisShardIterator iterator = kinesisShard.retrieveIterator(fromTimestamp("someShard", now));

        // then
        assertThat(iterator.getId(), is("someShardIterator"));

        GetShardIteratorRequest expectedRequest = GetShardIteratorRequest.builder()
                .streamName("someStream")
                .shardId("someShard")
                .shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                .timestamp(now)
                .build();
        verify(kinesisClient).getShardIterator(expectedRequest);
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
        final ShardPosition shardPosition = kinesisShard.consumeShard(fromHorizon("someShard"), now, consumer, (x) -> {});

        // then
        verify(consumer).accept(kinesisMessage("someShard", record1));
        verify(consumer).accept(kinesisMessage("someShard", record2));
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.position(), is("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldInterceptMessages() {
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
        interceptorChain.register((m) -> message(m.getKey(), m.getHeader(), "intercepted"));
        final ShardPosition shardPosition = kinesisShard.consumeShard(fromHorizon("someShard"), now, consumer, (x) -> {});

        // then
        verify(consumer).accept(message("first", responseHeader(fromPosition("someShard", "1"), future), "intercepted"));
        verify(consumer).accept(message("second", responseHeader(fromPosition("someShard", "2"), future), "intercepted"));
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.position(), is("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldIgnoreMessagesDroppedByInterceptor() {
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
        interceptorChain.register((m) ->
                m.getKey().equals("first")
                        ? null
                        : message(m.getKey(), m.getHeader(), "intercepted")
        );
        final ShardPosition shardPosition = kinesisShard.consumeShard(fromHorizon("someShard"), now, consumer, (x) -> {});

        // then
        verify(consumer).accept(message("second", responseHeader(fromPosition("someShard", "2"), future), "intercepted"));
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.position(), is("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldConsumeSingleRecordSetForStoppingThread() {
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
        kinesisShard.stop();
        final ShardPosition shardPosition = kinesisShard.consumeShard(fromHorizon("someShard"), now, consumer, (x) -> {});

        // then
        verify(consumer).accept(kinesisMessage("someShard", record1));
        verify(consumer).accept(kinesisMessage("someShard", record2));
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.position(), is("2"));
        assertThat(kinesisShard.isStopping(), is(true));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallCallbackForRecords() {
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
        kinesisShard.stop();
        final Consumer callback = mock(Consumer.class);
        kinesisShard.consumeShard(fromHorizon("someShard"), now, consumer, callback);

        // then
        verify(callback).accept(Duration.ofMillis(1234L));
        verifyNoMoreInteractions(callback);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallCallbackWithDurationBehindWithoutRecords() {
        // given
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        kinesisShard.stop();
        final Consumer callback = mock(Consumer.class);
        kinesisShard.consumeShard(fromPosition("someShard", "2"), now().minus(1, MILLIS), consumer, callback);

        // then
        verify(callback).accept(Duration.ofMillis(1234L));
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void shouldReturnEmptyPositionWhenThereAreNoRecords() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        ShardPosition shardPosition = kinesisShard.consumeShard(fromHorizon("someShard"), now(), consumer, (x) -> {});

        // then
        assertThat(shardPosition.position(), is(""));
    }

    @Test(expected = RuntimeException.class)
    public void shouldPropagateException() {
        // given
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenThrow(RuntimeException.class);

        // when
        kinesisShard.consumeShard(fromHorizon("someShard"), Instant.MAX, consumer, (x) -> {});

        // then
        // exception is thrown
    }

    @Test
    public void shouldCatchExceptionInConsumerAndCarryOn() {
        // given
        final Instant now = now();
        final Instant future = now.plus(1, SECONDS);
        final Record record1 = Record.builder()
                .partitionKey("first")
                .approximateArrivalTimestamp(future)
                .sequenceNumber("1")
                .build();
        final Record record2 = Record.builder()
                .partitionKey("second")
                .approximateArrivalTimestamp(future)
                .sequenceNumber("2")
                .build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record1, record2)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        final Message<String> kinesisMessage = kinesisMessage("someShard", record1);
        doThrow(new RuntimeException("forced exception for test")).when(consumer).accept(kinesisMessage);

        // when
        kinesisShard.consumeShard(fromHorizon("someShard"), now, consumer, (x) -> {});

        //then
        verify(consumer).accept(kinesisMessage("someShard", record1));
        verify(consumer).accept(kinesisMessage("someShard", record2));
    }
}
