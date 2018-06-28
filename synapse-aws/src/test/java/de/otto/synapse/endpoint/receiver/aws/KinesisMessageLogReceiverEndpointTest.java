package de.otto.synapse.endpoint.receiver.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.message.Message;
import de.otto.synapse.testsupport.TestClock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.matchingReceiverChannelsWith;
import static de.otto.synapse.endpoint.receiver.aws.KinesisShardIterator.POISON_SHARD_ITER;
import static de.otto.synapse.info.MessageReceiverStatus.*;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageLogReceiverEndpointTest {

    private static final Logger LOG = getLogger(KinesisMessageLogReceiverEndpointTest.class);

    private static final Pattern MATCH_ALL = Pattern.compile(".*");
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final TestClock clock = TestClock.now();

    @Mock
    private KinesisClient kinesisClient;

    @Captor
    private ArgumentCaptor<Message<String>> messageArgumentCaptor;
    @Mock
    private MessageConsumer<String> messageConsumer;

    private KinesisMessageLogReceiverEndpoint kinesisMessageLog;
    private AtomicInteger nextKey = new AtomicInteger(0);

    @Before
    public void setUp() {
        when(messageConsumer.keyPattern()).thenReturn(MATCH_ALL);
        when(messageConsumer.payloadType()).thenReturn(String.class);
    }

    @Test
    public void shouldRetrieveEmptyListOfShards() {
        // given
        describeStreamResponse(ImmutableList.of());
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper, null);

        // when
        List<KinesisShardReader> shards = kinesisMessageLog.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(0));
    }

    @Test
    public void shouldRetrieveSingleOpenShard() {
        // given
        describeStreamResponse(ImmutableList.of(someShard("shard1", true)));
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper, null);

        // when
        List<KinesisShardReader> shards = kinesisMessageLog.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(1));
        assertThat(shards.get(0).getShardName(), is("shard1"));
    }

    @Test
    public void shouldRetrieveOnlyOpenShards() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", false),
                        someShard("shard3", true)));
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);

        // when
        List<KinesisShardReader> shards = kinesisMessageLog.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardName(), is("shard1"));
        assertThat(shards.get(1).getShardName(), is("shard3"));
    }

    @Test
    public void shouldRetrieveShardsOfMultipleResponses() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", true)),
                ImmutableList.of(
                        someShard("shard3", true),
                        someShard("shard4", true)));
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);

        // when
        List<KinesisShardReader> shards = kinesisMessageLog.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(4));
        assertThat(shards.get(0).getShardName(), is("shard1"));
        assertThat(shards.get(1).getShardName(), is("shard2"));
        assertThat(shards.get(2).getShardName(), is("shard3"));
        assertThat(shards.get(3).getShardName(), is("shard4"));
    }

    @Test
    public void shouldRetrieveShardsOfMultipleResponsesWithFirstShardsClosed() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", false),
                        someShard("shard2", false)),
                ImmutableList.of(
                        someShard("shard3", true),
                        someShard("shard4", true)));
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);

        // when
        List<KinesisShardReader> shards = kinesisMessageLog.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardName(), is("shard3"));
        assertThat(shards.get(1).getShardName(), is("shard4"));
    }

    @Test
    public void shouldConsumeAllEventsFromKinesis() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1");

        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null, clock);
        kinesisMessageLog.register(messageConsumer);

        // when
        ChannelPosition finalChannelPosition = kinesisMessageLog.consume(fromHorizon());

        // then
        verify(messageConsumer, times(3)).accept(messageArgumentCaptor.capture());
        List<Message<String>> messages = messageArgumentCaptor.getAllValues();

        assertThat(messages.get(0).getPayload(), is("{\"data\":\"blue\"}"));
        assertThat(messages.get(1).getPayload(), is(nullValue()));
        assertThat(messages.get(2).getPayload(), is("{\"data\":\"green\"}"));
        assertThat(finalChannelPosition.shard("shard1").position(), is("2"));
    }

    @Test
    public void shouldConsumeAllMessagesFromMultipleShards() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", true),
                        someShard("shard3", true))
        );
        describeRecordsForShard("shard1");
        describeRecordsForShard("shard2");
        describeRecordsForShard("shard3");


        // when
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);
        kinesisMessageLog.register(messageConsumer);


        kinesisMessageLog.consume(fromHorizon());

        // then
        verify(messageConsumer, times(9)).accept(messageArgumentCaptor.capture());
        final List<Message<String>> allValues = messageArgumentCaptor.getAllValues();
        System.out.println(allValues);
    }


    @Test
    public void shouldInterceptMessages() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1");

        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("testStream", kinesisClient, objectMapper,null);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        // no lambda used in order to make Mockito happy...
        final MessageInterceptor interceptor = spy(new MessageInterceptor() {
            @Override
            public Message<String> intercept(Message<String> message) {
                return message;
            }
        });

        registry.register(matchingReceiverChannelsWith("testStream", interceptor));
        kinesisMessageLog.registerInterceptorsFrom(registry);
        kinesisMessageLog.register(messageConsumer);

        // when
        final ChannelPosition finalChannelPosition = kinesisMessageLog.consume(fromHorizon());

        // then
        verify(interceptor, atLeast(3)).intercept(any(Message.class));

        verify(messageConsumer, atLeast(3)).accept(messageArgumentCaptor.capture());
        List<Message<String>> messages = messageArgumentCaptor.getAllValues();

        assertThat(messages.get(0).getPayload(), is("{\"data\":\"blue\"}"));
        assertThat(messages.get(1).getPayload(), is(nullValue()));
        assertThat(messages.get(2).getPayload(), is("{\"data\":\"green\"}"));
        assertThat(finalChannelPosition.shard("shard1").position(), is("2"));
    }

    @Test
    public void shouldNotConsumeMessagesDroppedByInterceptor() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1");

        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("testStream", kinesisClient, objectMapper,null);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        // no lambda used in order to make Mockito happy...
        final MessageInterceptor interceptor = spy(new MessageInterceptor() {
            @Override
            public Message<String> intercept(Message<String> message) {
                return null;
            }
        });

        registry.register(matchingReceiverChannelsWith("testStream", interceptor));
        kinesisMessageLog.registerInterceptorsFrom(registry);
        kinesisMessageLog.register(messageConsumer);

        // when
        final ChannelPosition finalChannelPosition = kinesisMessageLog.consume(fromHorizon());

        // then
        verify(interceptor, atLeast(3)).intercept(any(Message.class));

        verifyZeroInteractions(messageConsumer);
        List<Message<String>> messages = messageArgumentCaptor.getAllValues();

        assertThat(messages, is(empty()));
        assertThat(finalChannelPosition.shard("shard1").position(), is("2"));
    }

    @Test
    public void shouldPublishEvents() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1");
        ArgumentCaptor<MessageReceiverNotification> eventCaptor = ArgumentCaptor.forClass(MessageReceiverNotification.class);
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("testStream", kinesisClient, objectMapper,eventPublisher);
        kinesisMessageLog.register(messageConsumer);

        // when
        final ChannelPosition finalChannelPosition = kinesisMessageLog.consume(fromHorizon());

        // then
        verify(eventPublisher, times(7)).publishEvent(eventCaptor.capture());
        List<MessageReceiverNotification> events = eventCaptor.getAllValues();

        assertThat(events.get(0).getStatus(), is(STARTING));
        assertThat(events.get(0).getChannelDurationBehind().isPresent(), is(false));
        assertThat(events.get(1).getStatus(), is(STARTED));
        assertThat(events.get(1).getChannelDurationBehind().isPresent(), is(false));
        assertThat(events.get(2).getStatus(), is(RUNNING)); // first emtpy record response, we dont evaluate millis behind latest from record response with zero records
        assertThat(events.get(2).getChannelDurationBehind().orElse(null), is(channelDurationBehind().with("shard1", ofMillis(555L)).build()));
        assertThat(events.get(3).getStatus(), is(RUNNING));
        assertThat(events.get(3).getChannelDurationBehind().orElse(null), is(channelDurationBehind().with("shard1", ofMillis(1234L)).build()));
        assertThat(events.get(4).getStatus(), is(RUNNING));
        assertThat(events.get(4).getChannelDurationBehind().orElse(null), is(channelDurationBehind().with("shard1", ZERO).build()));
        assertThat(events.get(5).getStatus(), is(RUNNING));
        assertThat(events.get(5).getChannelDurationBehind().orElse(null), is(channelDurationBehind().with("shard1", ZERO).build()));
        assertThat(events.get(6).getStatus(), is(FINISHED));
        assertThat(events.get(6).getChannelDurationBehind().isPresent(), is(false));

        assertThat(finalChannelPosition.shard("shard1").position(), is("2"));
    }

    @Test
    public void shouldShutdownOnStop() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1");

        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);

        kinesisMessageLog.register(messageConsumer);

        // when
        kinesisMessageLog.stop();
        ChannelPosition finalChannelPosition = kinesisMessageLog.consume(fromHorizon());

        // then
        assertThat(finalChannelPosition, is(channelPosition(fromHorizon("shard1"))));
    }

    @Test
    public void shouldStopShardsOnStop() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1");

        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);

        kinesisMessageLog.register(messageConsumer);

        // when
        kinesisMessageLog.stop();
        kinesisMessageLog.consume(fromHorizon());

        // then
        assertThat(kinesisMessageLog.getCurrentKinesisShards().size(), is(1));
        kinesisMessageLog.getCurrentKinesisShards().forEach(kinesisShardReader -> assertThat(kinesisShardReader.isStopping(), is(true)));
    }

    @Test(expected = RuntimeException.class)
    public void shouldShutdownOnException() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("failing-shard2", true))
        );
        describeRecordsForShard("shard1");
        describeRecordsForShard("failing-shard2");
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);

        kinesisMessageLog.register(messageConsumer);

        // when
        kinesisMessageLog.consume(fromHorizon());
    }

    @Test
    public void shouldBeAbleToRestartConsumeAfterException() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("failing-shard", true))
        );
        describeRecordsForShard("failing-shard");
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);
        kinesisMessageLog.register(messageConsumer);
        try {
            kinesisMessageLog.consume(fromHorizon());
        } catch (RuntimeException e) {
        }


        // when
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", true))
        );
        describeRecordsForShard("shard1");
        describeRecordsForShard("shard2");

        kinesisMessageLog.consume(fromHorizon());

        // then
        // 6 because of 2xshard1 + 1xshard2 - the failing-shard2 will not get messages
        verify(messageConsumer, times(6)).accept(messageArgumentCaptor.capture());
        final List<Message<String>> allValues = messageArgumentCaptor.getAllValues();
        System.out.println(allValues);
    }

    private Shard someShard(String shardId, boolean open) {
        return Shard.builder()
                .shardId(shardId)
                .sequenceNumberRange(SequenceNumberRange.builder()
                        .startingSequenceNumber("0000")
                        .endingSequenceNumber(open ? null : "1111")
                        .build())
                .build();
    }

    private void describeStreamResponse(List<Shard> shards) {
        DescribeStreamResponse response = createResponseForShards(shards, false);

        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(response);
    }

    private void describeStreamResponse(List<Shard> firstShardBatch, List<Shard> secondShardBatch) {
        DescribeStreamResponse firstResponse = createResponseForShards(firstShardBatch, true);
        DescribeStreamResponse secondResponse = createResponseForShards(secondShardBatch, false);

        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(firstResponse, secondResponse);
    }

    private void describeRecordsForShard(String shardName) {
        when(kinesisClient
                .getShardIterator(argThat((GetShardIteratorRequest req1) -> req1 != null && req1.shardId().equals(shardName))))
                .thenReturn(GetShardIteratorResponse.builder().shardIterator(shardName + "-iter").build());

        GetRecordsResponse response0 = GetRecordsResponse.builder()
                .records()
                .millisBehindLatest(555L)
                .nextShardIterator(shardName + "-pos1")
                .build();
        GetRecordsResponse response1 = GetRecordsResponse.builder()
                .records(
                        createRecord("blue"))
                .millisBehindLatest(1234L)
                .nextShardIterator(shardName + "-pos2")
                .build();
        GetRecordsResponse response2 = GetRecordsResponse.builder()
                .records(
                        createEmptyRecord(),
                        createRecord("green"))
                .millisBehindLatest(0L)
                .nextShardIterator(shardName + "-pos3")
                .build();
        GetRecordsResponse poison = GetRecordsResponse.builder()
                .records()
                .millisBehindLatest(0L)
                .nextShardIterator(POISON_SHARD_ITER)
                .build();

        when(kinesisClient.getRecords(argThat((GetRecordsRequest req) -> isFailingShardIter(shardName, req))))
                .thenThrow(new RuntimeException("boo!"));

        when(kinesisClient.getRecords(argThat((GetRecordsRequest req) -> isShardIter(shardName, req))))
                .thenReturn(response0, response1, response2, poison);
    }

    private boolean isShardIter(String shardName, GetRecordsRequest req) {
        return req != null && req.shardIterator().contains(shardName) && !req.shardIterator().contains("failing");
    }

    private boolean isFailingShardIter(String shardName, GetRecordsRequest req) {
        return req != null && req.shardIterator().contains(shardName) && req.shardIterator().contains("failing");
    }

    private DescribeStreamResponse createResponseForShards(List<Shard> shards, boolean hasMoreShards) {
        return DescribeStreamResponse.builder()
                .streamDescription(StreamDescription.builder()
                        .shards(shards)
                        .hasMoreShards(hasMoreShards)
                        .build())
                .build();
    }

    private Record createRecord(String data) {
        String json = "{\"data\":\"" + data + "\"}";
        final Record record = Record.builder()
                .partitionKey(data)
                .approximateArrivalTimestamp(clock.instant())
                .data(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)))
                .sequenceNumber(String.valueOf(nextKey.getAndIncrement()))
                .build();
        LOG.info("Created Record " + record);
        return record;
    }

    private Record createEmptyRecord() {
        final Record record = Record.builder()
                .partitionKey("empty")
                .approximateArrivalTimestamp(clock.instant())
                .data(ByteBuffer.allocateDirect(0))
                .sequenceNumber(String.valueOf(nextKey.getAndIncrement()))
                .build();
        LOG.info("Created Record " + record);
        return record;
    }

}
