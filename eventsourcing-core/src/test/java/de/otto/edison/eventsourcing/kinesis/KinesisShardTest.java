package de.otto.edison.eventsourcing.kinesis;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KinesisShardTest {

    @Mock
    private KinesisClient kinesisClient;

    @Mock
    private BiConsumer<Long, Record> consumer;

    @Mock
    BiFunction<Long, Record, Boolean> mockStopCondition;

    private KinesisShard kinesisShard;

    @Before
    public void setUp() {
        KinesisStream kinesisStream = new KinesisStream(kinesisClient, "someStream");
        kinesisShard = new KinesisShard("someShard", kinesisStream, kinesisClient);

        GetShardIteratorResponse fakeResponse = GetShardIteratorResponse.builder()
                .shardIterator("someShardIterator")
                .build();

        when(kinesisClient.getShardIterator(any())).thenReturn(fakeResponse);
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
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
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
        // given
        Record record1 = Record.builder()
                .sequenceNumber("1")
                .build();
        Record record2 = Record.builder()
                .sequenceNumber("2")
                .build();
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record1, record2)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any())).thenReturn(response);

        // when
        kinesisShard.consumeRecordsAndReturnLastSeqNumber("0", (x, y) -> true, consumer);

        // then
        verify(consumer).accept(1234L, record1);
        verify(consumer).accept(1234L, record2);
    }

    @Test
    public void shouldPassMillisBehindLatestToStopConditionWhenThereAreNoRecords() throws Exception {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any())).thenReturn(response);
        when(mockStopCondition.apply(any(), any())).thenReturn(true);

        // when
        kinesisShard.consumeRecordsAndReturnLastSeqNumber("0", mockStopCondition, consumer);

        // then
        verify(mockStopCondition).apply(1234L, null);
    }

    @Test
    public void shouldCatchExceptionInConsumerAndCarryOn() {
        // given
        Record record1 = Record.builder()
                .sequenceNumber("1")
                .build();
        Record record2 = Record.builder()
                .sequenceNumber("2")
                .build();
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record1, record2)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any())).thenReturn(response);

        doThrow(new RuntimeException("forced exception for test")).when(consumer).accept(1234L, record1);

        // when
        kinesisShard.consumeRecordsAndReturnLastSeqNumber("0", (x, y) -> true, consumer);

        //then
        verify(consumer).accept(1234L, record1);
        verify(consumer).accept(1234L, record2);
    }
}