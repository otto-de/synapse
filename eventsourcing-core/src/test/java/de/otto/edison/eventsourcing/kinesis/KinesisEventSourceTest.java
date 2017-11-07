package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class KinesisEventSourceTest {

    private static final String STREAM_NAME = "test";

    @Mock
    private KinesisUtils kinesisUtils;

    @Mock
    private KinesisStream kinesisStream;

    private ObjectMapper objectMapper = new ObjectMapper();
    private KinesisEventSource<TestData> eventSource;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.reset(kinesisUtils);

        eventSource = new KinesisEventSource<>(kinesisUtils, STREAM_NAME, TestData.class, objectMapper, kinesisStream);
    }

    @Test
    public void shouldConsumeAllEventsFromKinesis() throws Exception {
        withShards("shard1", "shard2");
        withInitialShardIterator("shard1", "shard1-iterator0");
        withInitialShardIterator("shard2", "shard2-iterator0");
        when(kinesisUtils.getRecords(anyString())).thenReturn(GetRecordsResponse.builder().millisBehindLatest(999L).nextShardIterator("latest").records(ImmutableList.of()).build());

        withGetRecordIterations(2, "shard1");
        withGetRecordIterations(2, "shard2");

        //when
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        StreamPosition streamPosition = eventSource.consumeAll(StreamPosition.of(),
                (stringEvent -> true),
                new EventConsumer<TestData>() {
                    @Override
                    public String streamName() {
                        return STREAM_NAME;
                    }

                    @Override
                    public void accept(Event<TestData> event) {
                        builder.add(event.payload().data);
                    }
                });

        //then
        assertThat(streamPosition.positionOf("shard1"), is("sequence-shard1-testdata0.2"));
        assertThat(streamPosition.positionOf("shard2"), is("sequence-shard2-testdata0.2"));
        assertThat(builder.build().size(), is(4));
    }

    @Test
    public void shouldStopConsumptionFromKinesis() throws Exception {
        withShards("shard1", "shard2");
        withInitialShardIterator("shard1", "shard1-iterator0");
        withInitialShardIterator("shard2", "shard2-iterator0");
        when(kinesisUtils.getRecords(anyString())).thenReturn(GetRecordsResponse.builder().millisBehindLatest(999L).nextShardIterator("latest").records(ImmutableList.of()).build());

        withGetRecordIterations(2, "shard1");
        withGetRecordIterations(2, "shard2");

        //when
        List<String> eventData = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean endOfShard1 = new AtomicBoolean(false);
        AtomicBoolean endOfShard2 = new AtomicBoolean(false);

        StreamPosition streamPosition = eventSource.consumeAll(StreamPosition.of(),
                (stringEvent -> endOfShard1.get() && endOfShard2.get()),
                new EventConsumer<TestData>() {
                    @Override
                    public String streamName() {
                        return STREAM_NAME;
                    }

                    @Override
                    public void accept(Event<TestData> event) {
                        eventData.add(event.payload().data);
                        if (event.sequenceNumber().equals("sequence-shard1-testdata1.2")) {
                            endOfShard1.set(true);
                        } else if (event.sequenceNumber().equals("sequence-shard2-testdata1.2")) {
                            endOfShard2.set(true);
                        }
                    }
                });
        //then
        assertThat(streamPosition.positionOf("shard1"), is("sequence-shard1-testdata1.2"));
        assertThat(streamPosition.positionOf("shard2"), is("sequence-shard2-testdata1.2"));
        assertThat(eventData.size(), is(8));
    }

    private void withInitialShardIterator(String shard, String initialShardIterator) {
        when(kinesisUtils.getShardIterator(STREAM_NAME, shard, "0")).thenReturn(initialShardIterator);
    }

    private void withShards(String... shards) {
        List<KinesisShard> shardList = Arrays.stream(shards)
                .map(KinesisShard::new)
                .collect(Collectors.toList());
        when(kinesisStream.retrieveAllOpenShards()).thenReturn(shardList);
    }

    private Record createRecord(String data) {
        String json = "{\"data\": \"" + data + "\"}";
        return Record.builder()
                .data(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)))
                .sequenceNumber("sequence-" + data)
                .build();
    }


    private void withGetRecordIterations(int count, String shard) {
        for (int i = 0; i < count; i++) {
            String shardIterator = shard + "-iterator" + i;
            when(kinesisUtils.getRecords(shardIterator)).thenReturn(GetRecordsResponse.builder()
                    .millisBehindLatest(1234L)
                    .records(createRecord(shard + "-testdata" + i + ".1"), createRecord(shard + "-testdata" + i + ".2"))
                    .nextShardIterator(shard + "-iterator" + (i + 1))
                    .build());
        }
    }

    public static class TestData {

        @JsonProperty
        public String data;

    }
}