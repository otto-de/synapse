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
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.when;

public class KinesisEventSourceTest {

    private static final String STREAM_NAME = "test";

    @Mock
    private KinesisUtils kinesisUtils;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldConsumeAllEventsFromKinesis() throws Exception {
        when(kinesisUtils.retrieveAllOpenShards(STREAM_NAME)).thenReturn(ImmutableList.of(Shard.builder().shardId("shard1").build(), Shard.builder().shardId("shard2").build()));
        when(kinesisUtils.getShardIterator(STREAM_NAME, "shard1", "0")).thenReturn("shardIterator1");
        when(kinesisUtils.getShardIterator(STREAM_NAME, "shard2", "0")).thenReturn("shardIterator2");
        when(kinesisUtils.getRecords("shardIterator1")).thenReturn(GetRecordsResponse.builder()
                .millisBehindLatest(1234L)
                .records(createRecord("testdata"), createRecord("testdata2"))
                .build());
        when(kinesisUtils.getRecords("shardIterator2")).thenReturn(GetRecordsResponse.builder()
                .millisBehindLatest(6789L)
                .records(createRecord("testdata3"), createRecord("testdata4"))
                .build());

        KinesisEventSource<TestData> eventSource = new KinesisEventSource<>(kinesisUtils, STREAM_NAME, TestData.class, objectMapper);

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
        assertThat(streamPosition.positionOf("shard1"), is("sequence-testdata2"));
        assertThat(streamPosition.positionOf("shard2"), is("sequence-testdata4"));
        assertThat(builder.build().size(), is(4));
    }

    private Record createRecord(String data) {
        String json = "{\"data\": \"" + data + "\"}";
        return Record.builder()
                .data(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)))
                .sequenceNumber("sequence-" + data)
                .build();
    }


    public static class TestData {

        @JsonProperty
        public String data;

    }
}