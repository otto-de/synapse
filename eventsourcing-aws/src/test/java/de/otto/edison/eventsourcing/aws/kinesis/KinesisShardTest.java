package de.otto.edison.eventsourcing.aws.kinesis;

import de.otto.edison.eventsourcing.consumer.MessageConsumer;
import de.otto.edison.eventsourcing.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.function.Predicate;

import static de.otto.edison.eventsourcing.aws.kinesis.KinesisMessage.kinesisMessage;
import static java.time.Duration.ofMillis;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
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
        // given
        Record record1 = Record.builder()
                .sequenceNumber("1")
                .partitionKey("first")
                .build();
        Record record2 = Record.builder()
                .sequenceNumber("2")
                .partitionKey("second")
                .build();
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record1, record2)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        final ShardResponse shardResponse = kinesisShard.consumeShard("0", (message) -> true, consumer);

        // then
        verify(consumer).accept(kinesisMessage(ofMillis(1234L), record1));
        verify(consumer).accept(kinesisMessage(ofMillis(1234L), record2));
        verifyNoMoreInteractions(consumer);

        assertThat(shardResponse.getStatus(), is(Status.STOPPED));
        assertThat(shardResponse.getShardPosition().getSequenceNumber(), is("2"));
    }

    @Test
    public void shouldPassMillisBehindLatestWithRecordToStopCondition() throws Exception {
        // given
        final Record record = Record.builder()
                .partitionKey("first")
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
        final ShardResponse shardResponse = kinesisShard.consumeShard("0", mockStopCondition, consumer);

        // then
        assertThat(shardResponse.getStatus(), is(Status.STOPPED));
        verify(mockStopCondition).test(
                kinesisMessage(ofMillis(1234L), record)
        );
    }

    @Test
    public void shouldReturnWithOkWhenThereAreNoRecords() throws Exception {
        // given
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        final ShardResponse shardResponse = kinesisShard.consumeShard("0", mockStopCondition, consumer);

        // then
        assertThat(shardResponse.getStatus(), is(Status.OK));
    }

    @Test(expected = RuntimeException.class)
    public void shouldPropagateException() throws Exception {
        // given
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenThrow(RuntimeException.class);

        // when
        kinesisShard.consumeShard("0", mockStopCondition, consumer);

        // then
        // exception is thrown
    }

    @Test
    public void shouldCatchExceptionInConsumerAndCarryOn() {
        // given
        Record record1 = Record.builder()
                .partitionKey("first")
                .sequenceNumber("1")
                .build();
        Record record2 = Record.builder()
                .partitionKey("second")
                .sequenceNumber("2")
                .build();
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record1, record2)
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        final Message<String> kinesisMessage = kinesisMessage(ofMillis(1234L), record1);
        doThrow(new RuntimeException("forced exception for test")).when(consumer).accept(kinesisMessage);

        // when
        kinesisShard.consumeShard("0", (message) -> true, consumer);

        //then
        verify(consumer).accept(kinesisMessage(ofMillis(1234L), record1));
        verify(consumer).accept(kinesisMessage(ofMillis(1234L), record2));
    }
}
