package de.otto.synapse.eventsource.aws;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.channel.Status;
import de.otto.synapse.channel.aws.KinesisMessageLogReceiverEndpoint;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.eventsource.EventSourceNotification;
import de.otto.synapse.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.Objects;
import java.util.function.Predicate;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class KinesisEventSourceTest {

    @Mock
    private KinesisMessageLogReceiverEndpoint kinesisMessageLog;

    @Mock
    private MessageConsumer<TestData> testDataConsumer;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        initMocks(this);
        when(kinesisMessageLog.getChannelName()).thenReturn("test");
        when(kinesisMessageLog.consume(any(ChannelPosition.class), any(Predicate.class), any(MessageConsumer.class)))
                .thenReturn(ChannelPosition.of(singletonMap("shard1", "4711")));
    }

    @Test
    public void shouldRegisterConsumer() {
        // given
        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisMessageLog, eventPublisher, objectMapper);

        // when
        eventSource.register(testDataConsumer);

        // then
        assertThat(eventSource.dispatchingMessageConsumer().getAll(), contains(testDataConsumer));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldConsumeAllEventsWithRegisteredConsumers() {
        // given
        ChannelPosition initialPositions = ChannelPosition.of(ImmutableMap.of("shard1", "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisMessageLog, eventPublisher, objectMapper);
        eventSource.register(testDataConsumer);
        eventSource.stop();

        // when
        eventSource.consumeAll(initialPositions, this::stopIfGreenForString);

        // then
        verify(kinesisMessageLog).consume(eq(initialPositions), any(Predicate.class), eq(eventSource.dispatchingMessageConsumer()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFinishConsumptionOnStopCondition() {
        // given
        ChannelPosition initialPositions = ChannelPosition.of(ImmutableMap.of("shard1", "xyz"));
        when(kinesisMessageLog.consume(any(ChannelPosition.class), any(Predicate.class), any(MessageConsumer.class)))
                .thenReturn(ChannelPosition.of(singletonMap("shard1", "4711")));


        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisMessageLog, eventPublisher, objectMapper);
        eventSource.register(testDataConsumer);

        // when
        final ChannelPosition channelPosition = eventSource.consumeAll(initialPositions, (message) -> true);

        // then
        assertThat(eventSource.isStopping(), is(false));
        assertThat(channelPosition, is(ChannelPosition.of(singletonMap("shard1", "4711"))));
    }

    @Test
    public void shouldFinishConsumptionOnStop() {
        // given
        ChannelPosition initialPositions = ChannelPosition.of(ImmutableMap.of("shard1", "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisMessageLog, eventPublisher, objectMapper);
        eventSource.register(testDataConsumer);

        // when
        eventSource.stop();
        eventSource.consumeAll(initialPositions, (message) -> false);

        // then
        assertThat(eventSource.isStopping(), is(true));
    }

    @Test
    public void shouldPublishStartedAndFinishedEvents() {
        // given
        ChannelPosition initialPositions = ChannelPosition.of(ImmutableMap.of("shard1", "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisMessageLog, eventPublisher, objectMapper);
        eventSource.stop();

        // when
        ChannelPosition finalChannelPosition = eventSource.consumeAll(initialPositions, (message) -> false);

        // then
        ArgumentCaptor<EventSourceNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(EventSourceNotification.class);
        verify(eventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

        EventSourceNotification startedEvent = notificationArgumentCaptor.getAllValues().get(0);
        assertThat(startedEvent.getStatus(), is(EventSourceNotification.Status.STARTED));
        assertThat(startedEvent.getChannelPosition(), is(initialPositions));
        assertThat(startedEvent.getStreamName(), is("test"));

        EventSourceNotification finishedEvent = notificationArgumentCaptor.getAllValues().get(1);
        assertThat(finishedEvent.getStatus(), is(EventSourceNotification.Status.FINISHED));
        assertThat(finishedEvent.getChannelPosition(), is(finalChannelPosition));
        assertThat(finishedEvent.getStreamName(), is("test"));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void shouldPublishStartedAndFailedEvents() {
        // given
        ChannelPosition initialPositions = ChannelPosition.of(ImmutableMap.of("shard1", "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", kinesisMessageLog, eventPublisher, objectMapper);
        when(kinesisMessageLog.consume(any(ChannelPosition.class), any(Predicate.class), any(MessageConsumer.class))).thenThrow(new RuntimeException("Error Message"));

        // when
        try {
            eventSource.consumeAll(initialPositions, this::stopIfGreenForString);
            fail("expected RuntimeException");
        } catch (final RuntimeException e) {
            // then
            ArgumentCaptor<EventSourceNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(EventSourceNotification.class);
            verify(eventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

            EventSourceNotification startedEvent = notificationArgumentCaptor.getAllValues().get(0);
            assertThat(startedEvent.getStatus(), is(EventSourceNotification.Status.STARTED));
            assertThat(startedEvent.getChannelPosition(), is(initialPositions));
            assertThat(startedEvent.getStreamName(), is("test"));

            EventSourceNotification failedEvent = notificationArgumentCaptor.getAllValues().get(1);
            assertThat(failedEvent.getStatus(), is(EventSourceNotification.Status.FAILED));
            assertThat(failedEvent.getMessage(), is("Error consuming messages from Kinesis: Error Message"));
            assertThat(failedEvent.getChannelPosition(), is(initialPositions));
            assertThat(failedEvent.getStreamName(), is("test"));
        }
    }


    private boolean stopIfGreenForString(Message<?> message) {
        return message.getPayload() != null && message.getPayload().toString().contains("green");
    }

    public static class TestData {

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
