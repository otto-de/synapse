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
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.time.Clock;
import java.time.Instant;
import java.util.function.Consumer;

import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
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

@RunWith(MockitoJUnitRunner.class)
public class KinesisShardReaderTest {

    @Mock
    private KinesisClient kinesisClient;

    @Mock
    private MessageConsumer<String> consumer;

    private InterceptorChain interceptorChain;
    private KinesisShardReader kinesisShardReader;

    @Before
    public void setUp() {
        interceptorChain = new InterceptorChain();
        kinesisShardReader = new KinesisShardReader("someStream", "someShard", kinesisClient, interceptorChain, Clock.systemDefaultZone());

        GetShardIteratorResponse fakeResponse = GetShardIteratorResponse.builder()
                .shardIterator("someShardIterator")
                .build();

        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(fakeResponse);
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
        final ShardPosition shardPosition = kinesisShardReader.consumeUntil(fromHorizon("someShard"), now, consumer);

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
        final ShardPosition shardPosition = kinesisShardReader.consumeUntil(fromHorizon("someShard"), now, consumer);

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
        final ShardPosition shardPosition = kinesisShardReader.consumeUntil(fromHorizon("someShard"), now, consumer);

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
        kinesisShardReader.stop();
        final ShardPosition shardPosition = kinesisShardReader.consumeUntil(fromHorizon("someShard"), now, consumer);

        // then
        verify(consumer).accept(kinesisMessage("someShard", record1));
        verify(consumer).accept(kinesisMessage("someShard", record2));
        verifyNoMoreInteractions(consumer);

        assertThat(shardPosition.position(), is("2"));
        assertThat(kinesisShardReader.isStopping(), is(true));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallCallbackForRecords() {
        final Consumer callback = mock(Consumer.class);
        kinesisShardReader = new KinesisShardReader("someStream", "someShard", kinesisClient, interceptorChain, Clock.systemDefaultZone()) {
            @Override
            public void afterBatch(final GetRecordsResponse response) {
                callback.accept(response);
            }
        };

        GetShardIteratorResponse fakeResponse = GetShardIteratorResponse.builder()
                .shardIterator("someShardIterator")
                .build();

        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(fakeResponse);

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

        kinesisShardReader.consumeUntil(fromHorizon("someShard"), now, consumer);

        // then
        verify(callback).accept(response);
        verifyNoMoreInteractions(callback);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallCallbackWithDurationBehindWithoutRecords() {
        // given
        final Consumer callback = mock(Consumer.class);
        kinesisShardReader = new KinesisShardReader("someStream", "someShard", kinesisClient, interceptorChain, Clock.systemDefaultZone()) {
            @Override
            public void afterBatch(final GetRecordsResponse response) {
                callback.accept(response);
            }
        };
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextShardIterator")
                .millisBehindLatest(1234L)
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        kinesisShardReader.stop();
        kinesisShardReader.consumeUntil(fromPosition("someShard", "2"), now().minus(1, MILLIS), consumer);

        // then
        verify(callback).accept(response);
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
        ShardPosition shardPosition = kinesisShardReader.consumeUntil(fromHorizon("someShard"), now(), consumer);

        // then
        assertThat(shardPosition.position(), is(""));
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
        kinesisShardReader.consumeUntil(fromHorizon("someShard"), now, consumer);

        //then
        verify(consumer).accept(kinesisMessage("someShard", record1));
        verify(consumer).accept(kinesisMessage("someShard", record2));
    }
}
