package de.otto.synapse.channel.aws;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
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

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.message.aws.KinesisMessage.kinesisMessage;
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

    private KinesisShard kinesisShard;

    @Before
    public void setUp() {
        kinesisShard = new KinesisShard("someShard", "someStream", kinesisClient);

        GetShardIteratorResponse fakeResponse = GetShardIteratorResponse.builder()
                .shardIterator("someShardIterator")
                .build();

        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(fakeResponse);
    }

    @Test
    public void shouldReturnTrimHorizonShardIteratorWhenStartPositionIsZero() {
        // when
        KinesisShardIterator iterator = kinesisShard.retrieveIterator("0");

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

        KinesisShardIterator iterator = kinesisShard.retrieveIterator("4711");

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
        KinesisShardIterator iterator = kinesisShard.retrieveIterator("1");

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
        final ChannelPosition channelPosition = kinesisShard.consumeRecords(fromHorizon(), (message) -> true, consumer);

        // then
        verify(consumer).accept(kinesisMessage("someShard", ofMillis(1234L), record1));
        verify(consumer).accept(kinesisMessage("someShard", ofMillis(1234L), record2));
        verifyNoMoreInteractions(consumer);

        assertThat(channelPosition.positionOf("someShard"), is("2"));
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
        ChannelPosition channelPosition = kinesisShard.consumeRecords(fromHorizon(), mockStopCondition, consumer);

        // then
        verify(mockStopCondition).test(kinesisMessage("someShard", ofMillis(1234L), record));
        assertThat(channelPosition.positionOf("someShard"), is("1"));
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
        ChannelPosition channelPosition = kinesisShard.consumeRecords(fromHorizon(), mockStopCondition, consumer);

        // then
        assertThat(channelPosition.positionOf("someShard"), is("0"));
    }

    @Test(expected = RuntimeException.class)
    public void shouldPropagateException() {
        // given
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenThrow(RuntimeException.class);

        // when
        kinesisShard.consumeRecords(fromHorizon(), mockStopCondition, consumer);

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
        kinesisShard.consumeRecords(fromHorizon(), (message) -> message.getKey().equals("second"), consumer);

        //then
        verify(consumer).accept(kinesisMessage("someShard", ofMillis(1234L), record1));
        verify(consumer).accept(kinesisMessage("someShard", ofMillis(1234L), record2));
    }
}
