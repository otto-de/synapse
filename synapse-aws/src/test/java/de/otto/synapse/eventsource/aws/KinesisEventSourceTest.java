package de.otto.synapse.eventsource.aws;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Objects;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
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
        receiverEndpoint = spy(new MessageLogReceiverEndpoint("test", new ObjectMapper(), eventPublisher) {
            @Nonnull
            @Override
            public ChannelPosition consumeUntil(@Nonnull ChannelPosition startFrom, @Nonnull Instant until) {
                return channelPosition(fromPosition("shard1", "4711"));
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
        ChannelPosition initialPositions = channelPosition(fromPosition("shard1", "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", receiverEndpoint);
        eventSource.register(testDataConsumer);
        eventSource.stop();

        // when
        eventSource.consumeUntil(initialPositions, now().plus(20, MILLIS));

        // then
        verify(receiverEndpoint).consumeUntil(eq(initialPositions), any(Instant.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFinishConsumptionOnStopCondition() {
        // given
        ChannelPosition initialPositions = channelPosition(fromPosition("shard1", "xyz"));
        receiverEndpoint = spy(new MessageLogReceiverEndpoint("test", new ObjectMapper(), eventPublisher) {
            @Nonnull
            @Override
            public ChannelPosition consumeUntil(@Nonnull ChannelPosition startFrom, @Nonnull Instant until) {
                return channelPosition(fromPosition("shard1", "4711"));
            }

            @Override
            public void stop() {
            }
        });


        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", receiverEndpoint);
        eventSource.register(testDataConsumer);

        // when
        final ChannelPosition channelPosition = eventSource.consume(initialPositions);

        // then
        assertThat(eventSource.isStopping(), is(false));
        assertThat(channelPosition, is(channelPosition(fromPosition("shard1", "4711"))));
    }

    @Test
    public void shouldFinishConsumptionOnStop() {
        // given
        ChannelPosition initialPositions = channelPosition(fromPosition("shard1", "xyz"));

        KinesisEventSource eventSource = new KinesisEventSource("kinesisEventSource", receiverEndpoint);
        eventSource.register(testDataConsumer);

        // when
        eventSource.stop();
        eventSource.consumeUntil(initialPositions, now());

        // then
        assertThat(eventSource.isStopping(), is(true));
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
