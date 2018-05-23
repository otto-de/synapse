package de.otto.synapse.channel.aws;

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

import java.time.Instant;
import java.util.function.Predicate;

import static de.otto.synapse.channel.ShardPosition.*;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static de.otto.synapse.message.aws.KinesisMessage.kinesisMessage;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static java.time.Instant.now;
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

    @Mock
    private Predicate<Message<?>> mockStopCondition;

    private InterceptorChain interceptorChain;
    private KinesisShard kinesisShard;

    @Before
    public void setUp() {
        interceptorChain = new InterceptorChain();
        kinesisShard = new KinesisShard("someShard", "someStream", kinesisClient, interceptorChain);

        GetShardIteratorResponse fakeResponse = GetShardIteratorResponse.builder()
                .shardIterator("someShardIterator")
                .build();

        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(fakeResponse);
    }

    @Test
    public void shouldReturnTrimHorizonShardIteratorWhenStartPositionIsZero() {
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

        KinesisShardIterator iterator = kinesisShard.retrieveIterator(fromPosition("someShard", ZERO, "4711"));

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
        KinesisShardIterator iterator = kinesisShard.retrieveIterator(fromPosition("someShard", ZERO, "1"));

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
        final Instant now = Instant.now();
        KinesisShardIterator iterator = kinesisShard.retrieveIterator(fromTimestamp("someShard", ZERO, now));

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
    public void shouldConsumeSingleRecordSetForStopAlwaysCondition() {
        final Instant now = now();
        // given
        final Record record1 = Record.builder()
                .sequenceNumber("1")
                .approximateArrivalTimestamp(now)
                .partitionKey("first")
                .build();
        final Record record2 = Record.builder()
                .sequenceNumber("2")
                .approximateArrivalTimestamp(now)
                .partitionKey("second")
                .build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record1, record2)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        final ShardPosition shardPosition = kinesisShard.consumeShard(ShardPosition.fromHorizon("someShard"), (message) -> true, consumer);

        // then
        verify(consumer).accept(kinesisMessage("someShard", ofMillis(1234L), record1));
        verify(consumer).accept(kinesisMessage("someShard", ofMillis(1234L), record2));
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.position(), is("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldInterceptMessages() {
        final Instant now = now();
        // given
        final Record record1 = Record.builder()
                .sequenceNumber("1")
                .approximateArrivalTimestamp(now)
                .partitionKey("first")
                .build();
        final Record record2 = Record.builder()
                .sequenceNumber("2")
                .approximateArrivalTimestamp(now)
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
        final ShardPosition shardPosition = kinesisShard.consumeShard(ShardPosition.fromHorizon("someShard"), (message) -> true, consumer);

        // then
        verify(consumer).accept(message("first", responseHeader(fromPosition("someShard", ofMillis(1234L), "1"), now), "intercepted"));
        verify(consumer).accept(message("second", responseHeader(fromPosition("someShard", ofMillis(1234L), "2"), now), "intercepted"));
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.position(), is("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldIgnoreMessagesDroppedByInterceptor() {
        final Instant now = now();
        // given
        final Record record1 = Record.builder()
                .sequenceNumber("1")
                .approximateArrivalTimestamp(now)
                .partitionKey("first")
                .build();
        final Record record2 = Record.builder()
                .sequenceNumber("2")
                .approximateArrivalTimestamp(now)
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
        final ShardPosition shardPosition = kinesisShard.consumeShard(ShardPosition.fromHorizon("someShard"), (message) -> true, consumer);

        // then
        verify(consumer).accept(message("second", responseHeader(fromPosition("someShard", ofMillis(1234L), "2"), now), "intercepted"));
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.position(), is("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldConsumeSingleRecordSetForStoppingThread() {
        final Instant now = now();
        // given
        final Record record1 = Record.builder()
                .sequenceNumber("1")
                .approximateArrivalTimestamp(now)
                .partitionKey("first")
                .build();
        final Record record2 = Record.builder()
                .sequenceNumber("2")
                .approximateArrivalTimestamp(now)
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
        final ShardPosition shardPosition = kinesisShard.consumeShard(ShardPosition.fromHorizon("someShard"), (message) -> false, consumer);

        // then
        verify(consumer).accept(kinesisMessage("someShard", ofMillis(1234L), record1));
        verify(consumer).accept(kinesisMessage("someShard", ofMillis(1234L), record2));
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.position(), is("2"));
        assertThat(kinesisShard.isStopping(), is(true));
    }

    @Test
    public void shouldPassMillisBehindLatestWithRecordToStopCondition() {
        // given
        final Instant now = now();
        final Record record = Record.builder()
                .partitionKey("first")
                .approximateArrivalTimestamp(now)
                .sequenceNumber("1")
                .build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(
                        record
                )
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);
        when(mockStopCondition.test(any())).thenReturn(true);

        // when
        ShardPosition shardPosition = kinesisShard.consumeShard(ShardPosition.fromHorizon("someShard"), mockStopCondition, consumer);

        // then
        verify(mockStopCondition).test(kinesisMessage("someShard", ofMillis(1234L), record));
        assertThat(shardPosition.position(), is("1"));
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
        when(mockStopCondition.test(any())).thenReturn(true);

        // when
        ShardPosition shardPosition = kinesisShard.consumeShard(ShardPosition.fromHorizon("someShard"), mockStopCondition, consumer);

        // then
        assertThat(shardPosition.position(), is(""));
    }

    @Test(expected = RuntimeException.class)
    public void shouldPropagateException() {
        // given
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenThrow(RuntimeException.class);

        // when
        kinesisShard.consumeShard(ShardPosition.fromHorizon("someShard"), mockStopCondition, consumer);

        // then
        // exception is thrown
    }

    @Test
    public void shouldCatchExceptionInConsumerAndCarryOn() {
        // given
        final Instant now = now();
        final Record record1 = Record.builder()
                .partitionKey("first")
                .approximateArrivalTimestamp(now)
                .sequenceNumber("1")
                .build();
        final Record record2 = Record.builder()
                .partitionKey("second")
                .approximateArrivalTimestamp(now)
                .sequenceNumber("2")
                .build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record1, record2)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        final Message<String> kinesisMessage = kinesisMessage("someShard", ofMillis(1234L), record1);
        doThrow(new RuntimeException("forced exception for test")).when(consumer).accept(kinesisMessage);

        // when
        kinesisShard.consumeShard(ShardPosition.fromHorizon("someShard"), (message) -> message.getKey().equals("second"), consumer);

        //then
        verify(consumer).accept(kinesisMessage("someShard", ofMillis(1234L), record1));
        verify(consumer).accept(kinesisMessage("someShard", ofMillis(1234L), record2));
    }
}
