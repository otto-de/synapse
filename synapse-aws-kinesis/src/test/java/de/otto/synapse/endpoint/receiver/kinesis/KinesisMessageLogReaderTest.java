package de.otto.synapse.endpoint.receiver.kinesis;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.testsupport.TestClock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.of;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.StopCondition.*;
import static de.otto.synapse.endpoint.receiver.kinesis.KinesisShardIterator.POISON_SHARD_ITER;
import static java.time.Duration.ofMillis;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageLogReaderTest {

    private static final Logger LOG = getLogger(KinesisMessageLogReaderTest.class);

    private static final TestClock clock = TestClock.now();

    @Mock
    private KinesisAsyncClient kinesisClient;

    @Captor
    private ArgumentCaptor<ShardResponse> responseArgumentCaptor;
    @Mock
    private Consumer<ShardResponse> responseConsumer;

    private ExecutorService executorService = Executors.newCachedThreadPool();
    private KinesisMessageLogReader logReader;

    private final AtomicInteger nextKey = new AtomicInteger(0);

    @Before
    public void before() {
        nextKey.set(0);
    }

    @Test
    public void shouldRetrieveEmptyListOfShards() {
        // given
        describeStreamResponse(of());
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        List<KinesisShardReader> shards = logReader.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(0));
    }

    @Test
    public void shouldRetrieveSingleOpenShard() {
        // given
        describeStreamResponse(of(someShard("shard1", true)));
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        List<KinesisShardReader> shards = logReader.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(1));
        assertThat(shards.get(0).getShardName(), is("shard1"));
    }

    @Test
    public void shouldGetOpenShards() {
        // given
        describeStreamResponse(of(
                someShard("shard1", true),
                someShard("shard2", false),
                someShard("shard3", true)
        ));
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        List<String> shards = logReader.getOpenShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards, containsInAnyOrder("shard1", "shard3"));
    }

    @Test
    public void shouldRetrieveOnlyOpenShards() {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true),
                        someShard("shard2", false),
                        someShard("shard3", true)));
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        List<KinesisShardReader> shards = logReader.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardName(), is("shard1"));
        assertThat(shards.get(1).getShardName(), is("shard3"));
    }

    @Test
    public void shouldRetrieveShardsOfMultipleResponses() {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true),
                        someShard("shard2", true)),
                of(
                        someShard("shard3", true),
                        someShard("shard4", true)),
                true);
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        List<KinesisShardReader> shards = logReader.getCurrentKinesisShards();

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
                of(
                        someShard("shard1", false),
                        someShard("shard2", false)),
                of(
                        someShard("shard3", true),
                        someShard("shard4", true)),
                true);
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        List<KinesisShardReader> shards = logReader.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardName(), is("shard3"));
        assertThat(shards.get(1).getShardName(), is("shard4"));
    }

    @Test
    public void shouldConsumeAllResponses() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", true);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        logReader.consumeUntil(fromHorizon(), shutdown(), responseConsumer).get();

        // then
        verify(responseConsumer, times(4)).accept(responseArgumentCaptor.capture());
        List<ShardResponse> responses = responseArgumentCaptor.getAllValues();

        assertThat(responses, hasSize(4));
        // first response:
        List<TextMessage> messages = responses.get(0).getMessages();
        assertThat(messages, is(empty()));
        // second response:
        messages = responses.get(1).getMessages();
        assertThat(messages, hasSize(1));
        assertThat(messages.get(0).getPayload(), is("{\"data\":\"blue\"}"));
        // third response:
        messages = responses.get(2).getMessages();
        assertThat(messages, hasSize(2));
        assertThat(messages.get(0).getPayload(), is(nullValue()));
        assertThat(messages.get(1).getPayload(), is("{\"data\":\"green\"}"));
        // fourth response:
        messages = responses.get(3).getMessages();
        assertThat(messages, is(empty()));
    }

    @Test
    public void shouldConsumeAllResponsesUntilEndOfChannel() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", false);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        ChannelPosition position = logReader.consumeUntil(fromHorizon(), endOfChannel(), responseConsumer).get();

        // then
        verify(responseConsumer, times(3)).accept(responseArgumentCaptor.capture());
        List<ShardResponse> responses = responseArgumentCaptor.getAllValues();

        assertThat(responses, hasSize(3));
        // first response:
        List<TextMessage> messages = responses.get(0).getMessages();
        assertThat(messages, is(empty()));
        // second response:
        messages = responses.get(1).getMessages();
        assertThat(messages, hasSize(1));
        assertThat(messages.get(0).getPayload(), is("{\"data\":\"blue\"}"));
        // third response:
        messages = responses.get(2).getMessages();
        assertThat(messages, hasSize(2));
        assertThat(messages.get(0).getPayload(), is(nullValue()));
        assertThat(messages.get(1).getPayload(), is("{\"data\":\"green\"}"));
    }

    @Test
    public void shouldConsumeAllResponsesFromMultipleShardsTilEndOfChannel() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true),
                        someShard("shard2", true)
                )
        );
        describeRecordsForShard("shard1", false);
        describeRecordsForShard("shard2", false);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        ChannelPosition position = logReader.consumeUntil(fromHorizon(), endOfChannel(), responseConsumer).get();

        // then
        verify(responseConsumer, times(6)).accept(responseArgumentCaptor.capture());
        List<ShardResponse> responses = responseArgumentCaptor.getAllValues();

        assertThat(responses, hasSize(6));
        assertThat(position, is(channelPosition(
                fromPosition("shard1", "2"),
                fromPosition("shard2", "5"))
        ));
    }

    @Test
    public void shouldIterateResponses() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", true);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        final KinesisMessageLogIterator iterator = logReader.getMessageLogIterator(fromHorizon());
        ChannelResponse response = logReader.read(iterator).get();
        // then (skip empty records)
        // assertThat(response.getMessages(), hasSize(0));
        // when
        // response = logReader.read(iterator).get();
        // then
        assertThat(response.getMessages(), hasSize(1));
        // when
        response = logReader.read(iterator).get();
        // then
        assertThat(response.getMessages(), hasSize(2));
        // when
        response = logReader.read(iterator).get();
        // then
        assertThat(response.getMessages(), hasSize(0));
    }

    @Test
    public void shouldIterateResponsesWithMultipleShards() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true),
                        someShard("shard2", true)));
        describeRecordsForShard("shard1", false);
        describeRecordsForShard("shard2", false);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        final KinesisMessageLogIterator iterator = logReader.getMessageLogIterator(fromHorizon());
        ChannelResponse response = logReader.read(iterator).get();
        // then (skip empty records)
        //        assertThat(response.getMessages(), hasSize(0));
        //        assertThat(response.getShardNames(), containsInAnyOrder("shard1", "shard2"));
        //        assertThat(response.getChannelDurationBehind().getDurationBehind(), is(ofMillis(555L)));
        //        assertThat(response.getChannelPosition(), is(
        //                channelPosition(
        //                        fromHorizon("shard1"),
        //                        fromHorizon("shard2"))
        //        ));
        // when
        // response = logReader.read(iterator).get();
        // then
        assertThat(response.getMessages(), hasSize(2));
        assertThat(response.getShardNames(), containsInAnyOrder("shard1", "shard2"));
        assertThat(response.getChannelDurationBehind().getDurationBehind(), is(ofMillis(1234L)));
        assertThat(response.getChannelPosition(), is(
                channelPosition(
                        fromPosition("shard1", "0"),
                        fromPosition("shard2", "3"))
        ));
        // when
        response = logReader.read(iterator).get();
        // then
        assertThat(response.getMessages(), hasSize(4));
        assertThat(response.getShardNames(), containsInAnyOrder("shard1", "shard2"));
        assertThat(response.getChannelDurationBehind().getDurationBehind(), is(ofMillis(0L)));
        assertThat(response.getChannelPosition(), is(
                channelPosition(
                        fromPosition("shard1", "2"),
                        fromPosition("shard2", "5"))
        ));
        // when
        response = logReader.read(iterator).get();
        // then
        assertThat(response.getMessages(), hasSize(0));
        assertThat(response.getChannelPosition(), is(
                channelPosition(
                        fromPosition("shard1", "2"),
                        fromPosition("shard2", "5"))
        ));
    }

    @Test
    public void shouldConsumeAllMessagesFromMultipleShards() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true),
                        someShard("shard2", true),
                        someShard("shard3", true))
        );
        describeRecordsForShard("shard1", true);
        describeRecordsForShard("shard2", true);
        describeRecordsForShard("shard3", true);


        // when
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        final CompletableFuture<ChannelPosition> futurePosition = logReader.consumeUntil(fromHorizon(), shutdown(), responseConsumer);
        futurePosition.get();

        // then
        verify(responseConsumer, times(12)).accept(responseArgumentCaptor.capture());

        final Set<String> shardNames = responseArgumentCaptor
                .getAllValues()
                .stream()
                .map(ShardResponse::getShardName)
                .collect(toSet());
        assertThat(shardNames, containsInAnyOrder("shard1", "shard2", "shard3"));
    }

    @Test
    public void shouldShutdownOnStop() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", false);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock, 1000, null);

        // when
        final CompletableFuture<ChannelPosition> finalChannelPosition = logReader.consumeUntil(fromHorizon(), shutdown(), responseConsumer);
        Thread.sleep(200);
        logReader.stop();

        // then
        finalChannelPosition.get(1, TimeUnit.SECONDS);
        assertThat(finalChannelPosition.isDone(), is(true));
        assertThat(logReader.getCurrentKinesisShards().get(0).isStopping(), is(true));
    }

    @Test
    public void shouldStopShardsOnStop() throws InterruptedException, ExecutionException, TimeoutException {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", false);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock, 1000, null);


        // when
        final CompletableFuture<ChannelPosition> futureChannelPosition = logReader.consumeUntil(fromHorizon(), shutdown(), responseConsumer);
        logReader.stop();
        futureChannelPosition.get(3, TimeUnit.SECONDS);
        // then
        assertThat(logReader.getCurrentKinesisShards().size(), is(1));
        logReader.getCurrentKinesisShards().forEach(kinesisShardReader -> assertThat(kinesisShardReader.isStopping(), is(true)));
    }

    @Test(expected = ExecutionException.class)
    public void shouldShutdownOnException() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                of(
                        someShard("shard1", true),
                        someShard("failing-shard2", true))
        );
        describeRecordsForShard("shard1", true);
        describeRecordsForShard("failing-shard2", true);
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);

        // when
        logReader.consumeUntil(fromHorizon(), shutdown(), responseConsumer).get();
    }

    @Test
    public void shouldBeAbleToRestartConsumeAfterException() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                of(
                        someShard("failing-shard", true)
                ),
                of(
                        someShard("shard1", true),
                        someShard("shard2", true)
                ),
                false
        );
        describeRecordsForShard("failing-shard", true);
        describeRecordsForShard("shard1", true);
        describeRecordsForShard("shard2", true);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, executorService, clock);
        try {
            logReader.consumeUntil(fromHorizon(), shutdown(), responseConsumer).get();
        } catch (ExecutionException e) {
        }

        // when
        logReader.consumeUntil(fromHorizon(), shutdown(), responseConsumer).get();

        // then
        verify(responseConsumer, times(8)).accept(responseArgumentCaptor.capture());
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

        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(completedFuture(response));
    }

    private void describeStreamResponse(List<Shard> firstShardBatch, List<Shard> secondShardBatch, boolean hasMoreShards) {
        DescribeStreamResponse firstResponse = createResponseForShards(firstShardBatch, hasMoreShards);
        DescribeStreamResponse secondResponse = createResponseForShards(secondShardBatch, false);

        when(kinesisClient.describeStream(any(DescribeStreamRequest.class)))
                .thenReturn(
                        completedFuture(firstResponse),
                        completedFuture(secondResponse));
    }

    private void describeRecordsForShard(final String shardName, boolean withPoison) {
        when(kinesisClient
                .getShardIterator(argThat((GetShardIteratorRequest req1) -> req1 != null && req1.shardId().equals(shardName))))
                .thenReturn(completedFuture(GetShardIteratorResponse.builder().shardIterator(shardName + "-iter").build()));

        GetRecordsResponse response0 = GetRecordsResponse.builder()
                .records(emptyList())
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
        GetRecordsResponse response3 = withPoison
                ? GetRecordsResponse.builder().records(emptyList()).millisBehindLatest(0L).nextShardIterator(POISON_SHARD_ITER).build()
                : GetRecordsResponse.builder().records(emptyList()).millisBehindLatest(0L).nextShardIterator(shardName + "-pos4").build();

        when(kinesisClient.getRecords(argThat((GetRecordsRequest req) -> isFailingShardIter(shardName, req))))
                .thenThrow(new RuntimeException("boo!"));

        when(kinesisClient.getRecords(argThat((GetRecordsRequest req) -> isShardIter(shardName, req))))
                .thenReturn(
                        completedFuture(response0),
                        completedFuture(response1),
                        completedFuture(response2),
                        completedFuture(response3));
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
                .data(SdkBytes.fromByteArray(json.getBytes(StandardCharsets.UTF_8)))
                .sequenceNumber(String.valueOf(nextKey.getAndIncrement()))
                .build();
        LOG.info("Created Record " + record);
        return record;
    }

    private Record createEmptyRecord() {
        final Record record = Record.builder()
                .partitionKey("empty")
                .approximateArrivalTimestamp(clock.instant())
                .data(SdkBytes.fromByteBuffer(ByteBuffer.allocateDirect(0)))
                .sequenceNumber(String.valueOf(nextKey.getAndIncrement()))
                .build();
        LOG.info("Created Record " + record);
        return record;
    }

}
