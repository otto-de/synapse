package de.otto.edison.eventsourcing.aws.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.event.Event;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.of;
import static java.util.Collections.synchronizedList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KinesisEventSourceTest {

    @Mock
    private KinesisStream kinesisStream;

    @Mock
    private KinesisClient kinesisClient;

    @Mock
    private EventConsumer<TestData> testDataConsumer;

    @Captor
    private ArgumentCaptor<Event<TestData>> testDataCaptor;

    @Mock
    private EventConsumer<String> stringConsumer;

    @Captor
    private ArgumentCaptor<Event<String>> stringCaptor;

    @Mock
    private Predicate<Event<?>> stringStopCondition;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    private ObjectMapper objectMapper = new ObjectMapper();

    private int nextKey = 0;


    @Before
    public void setUp() {
        KinesisShard shard1 = new KinesisShard("shard1", kinesisStream, kinesisClient);
        when(testDataConsumer.keyPattern()).thenReturn(Pattern.compile(".*"));
        when(testDataConsumer.payloadType()).thenReturn(TestData.class);
        when(stringConsumer.keyPattern()).thenReturn(Pattern.compile(".*"));
        when(stringConsumer.payloadType()).thenReturn(String.class);
        when(kinesisStream.getStreamName()).thenReturn("test");
        when(kinesisStream.retrieveAllOpenShards()).thenReturn(of(shard1));
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(GetShardIteratorResponse.builder()
                .shardIterator("someIterator")
                .build());

        GetRecordsResponse response0 = GetRecordsResponse.builder()
                .records()
                .millisBehindLatest(555L)
                .nextShardIterator("iterator1")
                .build();
        GetRecordsResponse response1 = GetRecordsResponse.builder()
                .records(createRecord("blue"))
                .millisBehindLatest(1234L)
                .nextShardIterator("nextIterator")
                .build();
        GetRecordsResponse response2 = GetRecordsResponse.builder()
                .records(createEmptyRecord(),
                        createRecord("green"))
                .millisBehindLatest(2345L)
                .nextShardIterator("yetAnotherIterator")
                .build();
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response0, response1, response2);
    }

    @Test
    public void shouldConsumeAllEventsFromKinesisWithObjectMapper() {
        // given
        StreamPosition initialPositions = StreamPosition.of(ImmutableMap.of("shard1", "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisStream, eventPublisher, objectMapper);
        eventSource.register(testDataConsumer);

        // when
        eventSource.consumeAll(initialPositions, this::stopIfGreenForString);

        // then
        verify(testDataConsumer, times(3)).accept(testDataCaptor.capture());
        List<Event<TestData>> events = testDataCaptor.getAllValues();

        assertThat(events.get(0).getEventBody().getPayload(), is(new TestData("blue")));
        assertThat(events.get(1).getEventBody().getPayload(), is(nullValue()));
        assertThat(events.get(2).getEventBody().getPayload(), is(new TestData("green")));
    }

    @Test
    public void shouldConsumeAllEventsFromKinesisAndPublishStartedAndFinishedEvents() {
        // given
        StreamPosition initialPositions = StreamPosition.of(ImmutableMap.of("shard1", "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisStream, eventPublisher, objectMapper);

        // when
        StreamPosition finalStreamPosition = eventSource.consumeAll(initialPositions, this::stopIfGreenForString);

        // then
        ArgumentCaptor<EventSourceNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(EventSourceNotification.class);
        verify(eventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

        EventSourceNotification startedEvent = notificationArgumentCaptor.getAllValues().get(0);
        assertThat(startedEvent.getStatus(), is(EventSourceNotification.Status.STARTED));
        assertThat(startedEvent.getStreamPosition(), is(initialPositions));
        assertThat(startedEvent.getStreamName(), is("test"));

        EventSourceNotification finishedEvent = notificationArgumentCaptor.getAllValues().get(1);
        assertThat(finishedEvent.getStatus(), is(EventSourceNotification.Status.FINISHED));
        assertThat(finishedEvent.getStreamPosition(), is(finalStreamPosition));
        assertThat(finishedEvent.getStreamName(), is("test"));
    }


    @Test
    public void shouldConsumeAllEventsAndDeserializeToString() throws Exception {
        // given
        StreamPosition initialPositions = StreamPosition.of(ImmutableMap.of("shard1", "xyz"));


        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisStream, eventPublisher, objectMapper);
        eventSource.register(stringConsumer);

        // when
        eventSource.consumeAll(initialPositions, this::stopIfGreenForString);

        // then
        verify(stringConsumer, times(3)).accept(stringCaptor.capture());

        List<Event<String>> events = stringCaptor.getAllValues();
        assertThat(events.get(0).getEventBody().getPayload(), is(objectMapper.writeValueAsString(new TestData("blue"))));
        assertThat(events.get(1).getEventBody().getPayload(), is(nullValue()));
        assertThat(events.get(2).getEventBody().getPayload(), is(objectMapper.writeValueAsString(new TestData("green"))));
    }

    @Test
    public void shouldAlwaysPassMillisBehindLatestToStopCondition() {
        // given
        StreamPosition initialPositions = StreamPosition.of(ImmutableMap.of("shard1", "xyz"));
        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisStream, eventPublisher, objectMapper);
        eventSource.register(stringConsumer);
        when(stringStopCondition.test(any())).thenReturn(true);

        // when
        eventSource.consumeAll(initialPositions, stringStopCondition);

        // then
        verify(stringStopCondition).test(Event.event(null, null, null, null, Duration.ofMillis(555L)));
    }

    @Test
    public void shouldFinishAllParallelThreadsWhenExceptionIsThrown() throws Exception {
        // given
        List<String> completedShards = synchronizedList(new ArrayList<>());

        List<KinesisShard> shards = IntStream.range(0, 1000)
                .mapToObj(i -> createShardMockWithSideEffect(i, () -> {
                            completedShards.add(String.valueOf(i));
                            if (i % 50 == 49) {
                                throw new RuntimeException("boom");
                            }
                        }
                ))
                .collect(Collectors.toList());

        when(kinesisStream.retrieveAllOpenShards()).thenReturn(shards);

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisStream, eventPublisher, objectMapper);
        eventSource.register(testDataConsumer);

        // when
        try {
            eventSource.consumeAll(StreamPosition.of());
            fail("exception expected");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), containsString("boom"));
            assertThat(completedShards.size(), not(0));
            completedShards.clear();
            ArgumentCaptor<EventSourceNotification> eventArgumentCaptor = ArgumentCaptor.forClass(EventSourceNotification.class);
            verify(eventPublisher, times(2)).publishEvent(eventArgumentCaptor.capture());
            assertThat(eventArgumentCaptor.getValue().getStatus(), is(EventSourceNotification.Status.FAILED));
            assertThat(eventArgumentCaptor.getValue().getMessage(), is("Error consuming messages from Kinesis: java.lang.RuntimeException: boom"));
        }
        Thread.sleep(100);

        //then
        assertThat(completedShards.size(), is(0));
    }

    private KinesisShard createShardMockWithSideEffect(int i, Runnable sideEffect) {
        KinesisShard shard = mock(KinesisShard.class);
        when(shard.getShardId()).thenReturn(String.valueOf(i));
        when(shard.consumeRecordsAndReturnLastSeqNumber(any(), any(), any())).thenAnswer(
                x -> {
                    sideEffect.run();
                    return (ShardPosition
                            .builder()
                            .withShardId(String.valueOf(i))
                            .withSequenceNumber(String.valueOf(i))
                            .build());
                }
        );
        return shard;
    }

    private boolean stopIfGreenForString(Event<?> event) {
        if (event.getEventBody().getPayload() == null) {
            return false;
        }
        return event.getEventBody().getPayload().toString().contains("green");
    }

    private Record createRecord(String data) {
        String json = "{\"data\":\"" + data + "\"}";
        return Record.builder()
                .partitionKey(String.valueOf(nextKey++))
                .data(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)))
                .sequenceNumber("sequence-" + data)
                .build();
    }

    private Record createEmptyRecord() {
        return Record.builder()
                .partitionKey(String.valueOf(nextKey++))
                .data(ByteBuffer.allocateDirect(0))
                .sequenceNumber("sequence-" + "empty")
                .build();
    }

    public static class TestData {

        TestData() {

        }

        public TestData(String data) {
            this.data = data;
        }

        @JsonProperty
        public String data;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestData testData = (TestData) o;
            return Objects.equals(data, testData.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }
    }
}
