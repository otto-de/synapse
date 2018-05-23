package de.otto.synapse.channel.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.info.MessageEndpointNotification;
import de.otto.synapse.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.matchingReceiverChannelsWith;
import static de.otto.synapse.info.MessageEndpointStatus.RUNNING;
import static de.otto.synapse.info.MessageEndpointStatus.STARTED;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageLogReceiverEndpointTest {

    private static final Pattern MATCH_ALL = Pattern.compile(".*");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private KinesisClient kinesisClient;

    @Captor
    private ArgumentCaptor<Message<String>> messageArgumentCaptor;
    @Mock
    private MessageConsumer<String> messageConsumer;

    private KinesisMessageLogReceiverEndpoint kinesisMessageLog;
    private int nextKey = 0;

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
        List<KinesisShard> shards = kinesisMessageLog.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(0));
    }

    @Test
    public void shouldRetrieveSingleOpenShard() {
        // given
        describeStreamResponse(ImmutableList.of(someShard("shard1", true)));
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper, null);

        // when
        List<KinesisShard> shards = kinesisMessageLog.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(1));
        assertThat(shards.get(0).getShardId(), is("shard1"));
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
        List<KinesisShard> shards = kinesisMessageLog.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardId(), is("shard1"));
        assertThat(shards.get(1).getShardId(), is("shard3"));
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
        List<KinesisShard> shards = kinesisMessageLog.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(4));
        assertThat(shards.get(0).getShardId(), is("shard1"));
        assertThat(shards.get(1).getShardId(), is("shard2"));
        assertThat(shards.get(2).getShardId(), is("shard3"));
        assertThat(shards.get(3).getShardId(), is("shard4"));
    }

    @Test
    public void shouldRetrieveShardsOfMultipleResponsesWithFirstAllClosed() {
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
        List<KinesisShard> shards = kinesisMessageLog.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardId(), is("shard3"));
        assertThat(shards.get(1).getShardId(), is("shard4"));
    }

    @Test
    public void shouldConsumeAllEventsFromKinesis() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", "iter1");

        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);
        kinesisMessageLog.register(messageConsumer);

        // when
        ChannelPosition finalChannelPosition = kinesisMessageLog.consume(ChannelPosition.fromHorizon(), this::stopIfGreenForString);

        // then
        verify(messageConsumer, times(3)).accept(messageArgumentCaptor.capture());
        List<Message<String>> messages = messageArgumentCaptor.getAllValues();

        assertThat(messages.get(0).getPayload(), is("{\"data\":\"blue\"}"));
        assertThat(messages.get(1).getPayload(), is(nullValue()));
        assertThat(messages.get(2).getPayload(), is("{\"data\":\"green\"}"));
        assertThat(finalChannelPosition.shard("shard1").position(), is("sequence-green"));
    }

    @Test
    public void shouldInterceptMessages() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", "iter1");

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
        final ChannelPosition finalChannelPosition = kinesisMessageLog.consume(ChannelPosition.fromHorizon(), this::stopIfGreenForString);

        // then
        verify(interceptor, times(3)).intercept(any(Message.class));

        verify(messageConsumer, times(3)).accept(messageArgumentCaptor.capture());
        List<Message<String>> messages = messageArgumentCaptor.getAllValues();

        assertThat(messages.get(0).getPayload(), is("{\"data\":\"blue\"}"));
        assertThat(messages.get(1).getPayload(), is(nullValue()));
        assertThat(messages.get(2).getPayload(), is("{\"data\":\"green\"}"));
        assertThat(finalChannelPosition.shard("shard1").position(), is("sequence-green"));
    }

    @Test
    public void shouldPublishEvents() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", "iter1");
        ArgumentCaptor<MessageEndpointNotification> eventCaptor = ArgumentCaptor.forClass(MessageEndpointNotification.class);
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("testStream", kinesisClient, objectMapper,eventPublisher);
        kinesisMessageLog.register(messageConsumer);

        // when
        final ChannelPosition finalChannelPosition = kinesisMessageLog.consume(ChannelPosition.fromHorizon(), this::stopIfGreenForString);

        // then

        verify(eventPublisher, times(5)).publishEvent(eventCaptor.capture());
        List<MessageEndpointNotification> events = eventCaptor.getAllValues();

        assertThat(events.get(0).getStatus(), is(STARTED));
        assertThat(events.get(0).getChannelPosition(), is(ChannelPosition.fromHorizon()));
        assertThat(events.get(1).getStatus(), is(RUNNING)); // Initial callback before do/while
        assertThat(events.get(1).getChannelPosition(), is(ChannelPosition.channelPosition(fromHorizon("shard1"))));;
        assertThat(events.get(2).getStatus(), is(RUNNING)); // first emtpy record response, we dont evaluate millis behind latest from record response with zero records
        assertThat(events.get(2).getChannelPosition(), is(ChannelPosition.channelPosition(fromHorizon("shard1"))));
        assertThat(events.get(3).getStatus(), is(RUNNING));
        assertThat(events.get(3).getChannelPosition(), is(ChannelPosition.channelPosition(fromPosition("shard1", Duration.ofMillis(1234L), "sequence-blue"))));
        assertThat(events.get(4).getStatus(), is(RUNNING));
        assertThat(events.get(4).getChannelPosition(), is(ChannelPosition.channelPosition(fromPosition("shard1", Duration.ofMillis(2345L), "sequence-green"))));
        assertThat(finalChannelPosition.shard("shard1").position(), is("sequence-green"));
    }

    @Test
    public void shouldShutdownOnStop() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", "iter1");

        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);

        kinesisMessageLog.register(messageConsumer);

        // when
        kinesisMessageLog.stop();
        ChannelPosition finalChannelPosition = kinesisMessageLog.consume(ChannelPosition.fromHorizon(), (m) -> false);

        // then
        assertThat(finalChannelPosition, is(channelPosition(fromHorizon("shard1"))));
    }

    @Test
    public void shouldStopShardsOnStop() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", "iter1");

        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);

        kinesisMessageLog.register(messageConsumer);

        // when
        kinesisMessageLog.stop();
        kinesisMessageLog.consume(ChannelPosition.fromHorizon(), (m) -> false);

        // then
        assertThat(kinesisMessageLog.getCurrentKinesisShards().size(), is(1));
        kinesisMessageLog.getCurrentKinesisShards().forEach(kinesisShard -> assertThat(kinesisShard.isStopping(), is(true)));
    }

    @Test(expected = RuntimeException.class)
    public void shouldShutdownOnException() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("failing-shard2", true))
        );
        describeRecordsForShard("shard1", "iter1");
        describeRecordsForShard("failing-shard2", "failing-iter2");
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);

        kinesisMessageLog.register(messageConsumer);

        // when
        kinesisMessageLog.consume(ChannelPosition.fromHorizon(), (m) -> false);
    }

    @Test
    public void shouldConsumeMessagesAfterException() {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("failing-shard2", true))
        );
        describeRecordsForShard("shard1", "iter1");
        describeRecordsForShard("failing-shard2", "failing-iter2");
        kinesisMessageLog = new KinesisMessageLogReceiverEndpoint("channelName", kinesisClient, objectMapper,null);
        kinesisMessageLog.register(messageConsumer);

        // when
        try {
            kinesisMessageLog.consume(ChannelPosition.fromHorizon(), (m) -> false);
        } catch (RuntimeException e) {

        }
        describeRecordsForShard("failing-shard2", "iter2");
        ChannelPosition finalChannelPosition = kinesisMessageLog.consume(ChannelPosition.fromHorizon(), this::stopIfGreenForString);

        // then
        verify(messageConsumer, times(6)).accept(messageArgumentCaptor.capture());
        List<Message<String>> messages = messageArgumentCaptor.getAllValues();

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

    private void describeRecordsForShard(String shardName, String iteratorName) {
        when(kinesisClient.getShardIterator(argThat((GetShardIteratorRequest req) -> req != null && req.shardId().equals
                (shardName))))
                .thenReturn(
                GetShardIteratorResponse.builder()
                .shardIterator(iteratorName)
                .build());

        GetRecordsResponse response0 = GetRecordsResponse.builder()
                .records()
                .millisBehindLatest(555L)
                .nextShardIterator("iterator1")
                .build();
        GetRecordsResponse response1 = GetRecordsResponse.builder()
                .records(
                        createRecord("blue"))
                .millisBehindLatest(1234L)
                .nextShardIterator("nextIterator")
                .build();
        GetRecordsResponse response2 = GetRecordsResponse.builder()
                .records(
                        createEmptyRecord(),
                        createRecord("green"))
                .millisBehindLatest(2345L)
                .nextShardIterator("yetAnotherIterator")
                .build();

        when(kinesisClient.getRecords(argThat((GetRecordsRequest req) -> req != null && req.shardIterator().contains("failing"))))
                .thenThrow(new RuntimeException("boo!"));

        when(kinesisClient.getRecords(argThat((GetRecordsRequest req) -> req == null || !req.shardIterator().contains("failing"))))
                .thenReturn(response0, response1, response2);
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
        return Record.builder()
                .partitionKey(String.valueOf(nextKey++))
                .approximateArrivalTimestamp(Instant.now())
                .data(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)))
                .sequenceNumber("sequence-" + data)
                .build();
    }

    private Record createEmptyRecord() {
        return Record.builder()
                .partitionKey(String.valueOf(nextKey++))
                .approximateArrivalTimestamp(Instant.now())
                .data(ByteBuffer.allocateDirect(0))
                .sequenceNumber("sequence-" + "empty")
                .build();
    }


    private boolean stopIfGreenForString(Message<?> message) {
        return message.getPayload() != null && message.getPayload().toString().contains("green");
    }

}
