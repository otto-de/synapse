package de.otto.synapse.eventsource.aws;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.info.MessageEndpointNotification;
import de.otto.synapse.info.MessageEndpointStatus;
import de.otto.synapse.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class KinesisEventSourceTest {

    private MessageLogReceiverEndpoint receiverEndpoint;

    @Mock
    private MessageConsumer<TestData> testDataConsumer;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        initMocks(this);
        receiverEndpoint = spy(new MessageLogReceiverEndpoint("test", new ObjectMapper()) {
            @Nonnull
            @Override
            public ChannelPosition consume(@Nonnull ChannelPosition startFrom, @Nonnull Predicate<Message<?>> stopCondition) {
                return channelPosition(fromPosition("shard1", Duration.ZERO, "4711"));
            }

            @Override
            public void stop() {
            }
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldConsumeAllEventsWithRegisteredConsumers() {
        // given
        ChannelPosition initialPositions = channelPosition(fromPosition("shard1", Duration.ZERO, "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", receiverEndpoint, eventPublisher);
        eventSource.register(testDataConsumer);
        eventSource.stop();

        // when
        eventSource.consume(initialPositions, this::stopIfGreenForString);

        // then
        verify(receiverEndpoint).consume(eq(initialPositions), any(Predicate.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFinishConsumptionOnStopCondition() {
        // given
        ChannelPosition initialPositions = channelPosition(fromPosition("shard1", Duration.ZERO, "xyz"));
        receiverEndpoint = spy(new MessageLogReceiverEndpoint("test", new ObjectMapper()) {
            @Nonnull
            @Override
            public ChannelPosition consume(@Nonnull ChannelPosition startFrom, @Nonnull Predicate<Message<?>> stopCondition) {
                return channelPosition(fromPosition("shard1", Duration.ZERO, "4711"));
            }

            @Override
            public void stop() {
            }
        });


        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", receiverEndpoint, eventPublisher);
        eventSource.register(testDataConsumer);

        // when
        final ChannelPosition channelPosition = eventSource.consume(initialPositions, (message) -> true);

        // then
        assertThat(eventSource.isStopping(), is(false));
        assertThat(channelPosition, is(channelPosition(fromPosition("shard1", Duration.ZERO, "4711"))));
    }

    @Test
    public void shouldFinishConsumptionOnStop() {
        // given
        ChannelPosition initialPositions = channelPosition(fromPosition("shard1", Duration.ZERO, "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", receiverEndpoint, eventPublisher);
        eventSource.register(testDataConsumer);

        // when
        eventSource.stop();
        eventSource.consume(initialPositions, (message) -> false);

        // then
        assertThat(eventSource.isStopping(), is(true));
    }

    @Test
    public void shouldPublishStartedAndFinishedEvents() {
        // given
        ChannelPosition initialPositions = channelPosition(fromPosition("shard1", Duration.ZERO, "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", receiverEndpoint, eventPublisher);
        eventSource.stop();

        // when
        ChannelPosition finalChannelPosition = eventSource.consume(initialPositions, (message) -> false);

        // then
        ArgumentCaptor<MessageEndpointNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(MessageEndpointNotification.class);
        verify(eventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

        MessageEndpointNotification startedEvent = notificationArgumentCaptor.getAllValues().get(0);
        assertThat(startedEvent.getStatus(), is(MessageEndpointStatus.STARTING));
        assertThat(startedEvent.getChannelPosition(), is(initialPositions));
        assertThat(startedEvent.getChannelName(), is("test"));

        MessageEndpointNotification finishedEvent = notificationArgumentCaptor.getAllValues().get(1);
        assertThat(finishedEvent.getStatus(), is(MessageEndpointStatus.FINISHED));
        assertThat(finishedEvent.getChannelPosition(), is(finalChannelPosition));
        assertThat(finishedEvent.getChannelName(), is("test"));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void shouldPublishStartedAndFailedEvents() {
        // given
        ChannelPosition initialPositions = channelPosition(fromPosition("shard1", Duration.ZERO, "xyz"));

        receiverEndpoint = spy(new MessageLogReceiverEndpoint("test", new ObjectMapper()) {
            @Nonnull
            @Override
            public ChannelPosition consume(@Nonnull ChannelPosition startFrom, @Nonnull Predicate<Message<?>> stopCondition) {
                throw new RuntimeException("Some Error Message");
            }

            @Override
            public void stop() {
            }
        });
        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", receiverEndpoint, eventPublisher);

        // when
        try {
            eventSource.consume(initialPositions, this::stopIfGreenForString);
            fail("expected RuntimeException");
        } catch (final RuntimeException e) {
            // then
            ArgumentCaptor<MessageEndpointNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(MessageEndpointNotification.class);
            verify(eventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

            MessageEndpointNotification startedEvent = notificationArgumentCaptor.getAllValues().get(0);
            assertThat(startedEvent.getStatus(), is(MessageEndpointStatus.STARTING));
            assertThat(startedEvent.getChannelPosition(), is(initialPositions));
            assertThat(startedEvent.getChannelName(), is("test"));

            MessageEndpointNotification failedEvent = notificationArgumentCaptor.getAllValues().get(1);
            assertThat(failedEvent.getStatus(), is(MessageEndpointStatus.FAILED));
            assertThat(failedEvent.getMessage(), is("Error consuming messages from Kinesis: Some Error Message"));
            assertThat(failedEvent.getChannelPosition(), is(initialPositions));
            assertThat(failedEvent.getChannelName(), is("test"));
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
