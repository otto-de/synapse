package de.otto.synapse.endpoint.receiver.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;
import de.otto.synapse.testsupport.TestClock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.endpoint.receiver.aws.KinesisShardIterator.POISON_SHARD_ITER;
import static java.time.Duration.ofMillis;
import static java.util.Collections.emptyList;
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

    private static final Pattern MATCH_ALL = Pattern.compile(".*");
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final TestClock clock = TestClock.now();

    @Mock
    private KinesisClient kinesisClient;

    @Captor
    private ArgumentCaptor<KinesisShardResponse> responseArgumentCaptor;
    @Mock
    private Consumer<KinesisShardResponse> responseConsumer;

    private KinesisMessageLogReader logReader;
    private AtomicInteger nextKey = new AtomicInteger(0);

    @Test
    public void shouldRetrieveEmptyListOfShards() {
        // given
        describeStreamResponse(ImmutableList.of());
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

        // when
        List<KinesisShardReader> shards = logReader.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(0));
    }

    @Test
    public void shouldRetrieveSingleOpenShard() {
        // given
        describeStreamResponse(ImmutableList.of(someShard("shard1", true)));
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

        // when
        List<KinesisShardReader> shards = logReader.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(1));
        assertThat(shards.get(0).getShardName(), is("shard1"));
    }

    @Test
    public void shouldGetOpenShards() {
        // given
        describeStreamResponse(ImmutableList.of(
                someShard("shard1", true),
                someShard("shard2", false),
                someShard("shard3", true)
        ));
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

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
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", false),
                        someShard("shard3", true)));
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

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
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", true)),
                ImmutableList.of(
                        someShard("shard3", true),
                        someShard("shard4", true)));
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

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
                ImmutableList.of(
                        someShard("shard1", false),
                        someShard("shard2", false)),
                ImmutableList.of(
                        someShard("shard3", true),
                        someShard("shard4", true)));
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

        // when
        List<KinesisShardReader> shards = logReader.getCurrentKinesisShards();

        // then
        assertThat(shards, hasSize(2));
        assertThat(shards.get(0).getShardName(), is("shard3"));
        assertThat(shards.get(1).getShardName(), is("shard4"));
    }

    @Test
    public void shouldConsumeAllResponsesFromKinesis() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", true);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

        // when
        ChannelPosition position = logReader.consumeUntil(fromHorizon(), Instant.MAX, responseConsumer).get();

        // then
        verify(responseConsumer, times(4)).accept(responseArgumentCaptor.capture());
        List<KinesisShardResponse> responses = responseArgumentCaptor.getAllValues();

        assertThat(responses, hasSize(4));
        // first response:
        List<Message<String>> messages = responses.get(0).getMessages();
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
    public void shouldIterateResponses() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", true);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

        // when
        final KinesisMessageLogIterator iterator = logReader.getMessageLogIterator(fromHorizon());
        KinesisMessageLogResponse response = logReader.read(iterator).get();
        // then
        assertThat(response.getMessages(), hasSize(0));
        // when
        response = logReader.read(iterator).get();
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
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", true)));
        describeRecordsForShard("shard1", false);
        describeRecordsForShard("shard2", false);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

        // when
        final KinesisMessageLogIterator iterator = logReader.getMessageLogIterator(fromHorizon());
        KinesisMessageLogResponse response = logReader.read(iterator).get();
        // then
        assertThat(response.getMessages(), hasSize(0));
        assertThat(response.getShardNames(), containsInAnyOrder("shard1", "shard2"));
        assertThat(response.getChannelDurationBehind().getDurationBehind(), is(ofMillis(555L)));
        assertThat(response.getChannelPosition(), is(
                channelPosition(
                        fromHorizon("shard1"),
                        fromHorizon("shard2"))
        ));
        // when
        response = logReader.read(iterator).get();
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
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", true),
                        someShard("shard3", true))
        );
        describeRecordsForShard("shard1", true);
        describeRecordsForShard("shard2", true);
        describeRecordsForShard("shard3", true);


        // when
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

        final CompletableFuture<ChannelPosition> futurePosition = logReader.consumeUntil(fromHorizon(), Instant.MAX, responseConsumer);
        futurePosition.get();

        // then
        verify(responseConsumer, times(12)).accept(responseArgumentCaptor.capture());

        final Set<String> shardNames = responseArgumentCaptor
                .getAllValues()
                .stream()
                .map(KinesisShardResponse::getShardName)
                .collect(toSet());
        assertThat(shardNames, containsInAnyOrder("shard1", "shard2", "shard3"));
    }

    @Test
    public void shouldShutdownOnStop() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", false);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

        // when
        final CompletableFuture<ChannelPosition> finalChannelPosition = logReader.consumeUntil(fromHorizon(), Instant.MAX, responseConsumer);
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
                ImmutableList.of(
                        someShard("shard1", true)));
        describeRecordsForShard("shard1", false);

        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);


        // when
        final CompletableFuture<ChannelPosition> futureChannelPosition = logReader.consumeUntil(fromHorizon(), Instant.MAX, responseConsumer);
        logReader.stop();
        futureChannelPosition.get(1, TimeUnit.SECONDS);
        // then
        assertThat(logReader.getCurrentKinesisShards().size(), is(1));
        logReader.getCurrentKinesisShards().forEach(kinesisShardReader -> assertThat(kinesisShardReader.isStopping(), is(true)));
    }

    @Test(expected = ExecutionException.class)
    public void shouldShutdownOnException() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("failing-shard2", true))
        );
        describeRecordsForShard("shard1", true);
        describeRecordsForShard("failing-shard2", true);
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);

        // when
        logReader.consumeUntil(fromHorizon(), Instant.MAX, responseConsumer).get();
    }

    @Test
    public void shouldBeAbleToRestartConsumeAfterException() throws ExecutionException, InterruptedException {
        // given
        describeStreamResponse(
                ImmutableList.of(
                        someShard("failing-shard", true))
        );
        describeRecordsForShard("failing-shard", true);
        logReader = new KinesisMessageLogReader("channelName", kinesisClient, clock, true);
        try {
            logReader.consumeUntil(fromHorizon(), Instant.MAX, responseConsumer).get();
        } catch (ExecutionException e) {
        }


        // when
        describeStreamResponse(
                ImmutableList.of(
                        someShard("shard1", true),
                        someShard("shard2", true))
        );
        describeRecordsForShard("shard1", true);
        describeRecordsForShard("shard2", true);

        logReader.consumeUntil(fromHorizon(), Instant.MAX, responseConsumer).get();

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

        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(response);
    }

    private void describeStreamResponse(List<Shard> firstShardBatch, List<Shard> secondShardBatch) {
        DescribeStreamResponse firstResponse = createResponseForShards(firstShardBatch, true);
        DescribeStreamResponse secondResponse = createResponseForShards(secondShardBatch, false);

        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(firstResponse, secondResponse);
    }

    private void describeRecordsForShard(final String shardName, boolean withPoison) {
        when(kinesisClient
                .getShardIterator(argThat((GetShardIteratorRequest req1) -> req1 != null && req1.shardId().equals(shardName))))
                .thenReturn(GetShardIteratorResponse.builder().shardIterator(shardName + "-iter").build());

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
                : GetRecordsResponse.builder().records(emptyList()).millisBehindLatest(0L).nextShardIterator(shardName + "-pos3").build();

        when(kinesisClient.getRecords(argThat((GetRecordsRequest req) -> isFailingShardIter(shardName, req))))
                .thenThrow(new RuntimeException("boo!"));

        when(kinesisClient.getRecords(argThat((GetRecordsRequest req) -> isShardIter(shardName, req))))
                .thenReturn(response0, response1, response2, response3);
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
