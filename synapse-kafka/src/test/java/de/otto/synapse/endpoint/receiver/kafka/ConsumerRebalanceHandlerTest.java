package de.otto.synapse.endpoint.receiver.kafka;

import de.otto.synapse.channel.ChannelPosition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;

import static com.google.common.collect.ImmutableMap.of;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.*;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.STARTED;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class ConsumerRebalanceHandlerTest {

    @Test
    public void shouldInitiallyHaveNotAssignedAndPositionedShards() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final Consumer<String, String> consumer = mock(Consumer.class);

        final ConsumerRebalanceHandler handler = new ConsumerRebalanceHandler(
                "foo",
                ChannelPosition.fromHorizon(),
                eventPublisher,
                consumer);

        // then
        assertThat(handler.shardsAssignedAndPositioned(), is(false));
    }

    @Test
    public void shouldHaveAssignedAndPositionedShardsAfterAssignment() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final Consumer<String, String> consumer = mock(Consumer.class);

        final ConsumerRebalanceHandler handler = new ConsumerRebalanceHandler(
                "foo",
                ChannelPosition.fromHorizon(),
                eventPublisher,
                consumer);

        // when
        handler.onPartitionsAssigned(asList(
                new TopicPartition("foo", 0),
                new TopicPartition("foo", 1)
        ));
        // then
        assertThat(handler.shardsAssignedAndPositioned(), is(true));
        assertThat(handler.getCurrentPartitions(), contains("0", "1"));
    }

    @Test
    public void shouldSendApplicationEventAfterAssignment() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final Consumer<String, String> consumer = mock(Consumer.class);

        final ConsumerRebalanceHandler handler = new ConsumerRebalanceHandler(
                "foo",
                ChannelPosition.fromHorizon(),
                eventPublisher,
                consumer);

        // when
        handler.onPartitionsAssigned(asList(
                new TopicPartition("foo", 0),
                new TopicPartition("foo", 1)
        ));
        // then
        verify(eventPublisher).publishEvent(builder()
                .withChannelName("foo")
                .withStatus(STARTED)
                .withMessage("Received shards from Kafka.")
                .build());
    }

    @Test
    public void shouldSeekToBeginningAfterAssignmentStartingFromHorizon() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final Consumer<String, String> consumer = mock(Consumer.class);

        final ConsumerRebalanceHandler handler = new ConsumerRebalanceHandler(
                "foo",
                ChannelPosition.fromHorizon(),
                eventPublisher,
                consumer);

        // when
        handler.onPartitionsAssigned(asList(
                new TopicPartition("foo", 0),
                new TopicPartition("foo", 1)
        ));

        // then
        verify(consumer).seekToBeginning(asList(new TopicPartition("foo", 0)));
        verify(consumer).seekToBeginning(asList(new TopicPartition("foo", 1)));
    }

    @Test
    public void shouldSeekToPositionAfterAssignmentStartingFromPosition() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final Consumer<String, String> consumer = mock(Consumer.class);

        final ConsumerRebalanceHandler handler = new ConsumerRebalanceHandler(
                "foo",
                channelPosition(fromPosition("0", "42")),
                eventPublisher,
                consumer);

        // when
        handler.onPartitionsAssigned(asList(
                new TopicPartition("foo", 0),
                new TopicPartition("foo", 1)
        ));

        // then
        verify(consumer).seek(new TopicPartition("foo", 0), 43);
        verify(consumer).seekToBeginning(asList(new TopicPartition("foo", 1)));
    }

    @Test
    public void shouldSeekToPositionAfterAssignmentStartingAtPosition() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final Consumer<String, String> consumer = mock(Consumer.class);

        final ConsumerRebalanceHandler handler = new ConsumerRebalanceHandler(
                "foo",
                channelPosition(
                        atPosition("0", "42"),
                        atPosition("1", "4711")),
                eventPublisher,
                consumer);

        // when
        handler.onPartitionsAssigned(asList(
                new TopicPartition("foo", 0),
                new TopicPartition("foo", 1)
        ));

        // then
        verify(consumer).seek(new TopicPartition("foo", 0), 42);
        verify(consumer).seek(new TopicPartition("foo", 1), 4711);
    }

    @Test
    public void shouldSeekToPositionAfterAssignmentStartingFromTimestamp() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final Consumer<String, String> consumer = mock(Consumer.class);
        when(consumer.offsetsForTimes(of(
                new TopicPartition("foo", 0), 42L)))
                .thenReturn(of(
                        new TopicPartition("foo", 0), new OffsetAndTimestamp(4711L, 42L))
                );

        final ConsumerRebalanceHandler handler = new ConsumerRebalanceHandler(
                "foo",
                channelPosition(fromTimestamp("0", ofEpochMilli(42))),
                eventPublisher,
                consumer);

        // when
        handler.onPartitionsAssigned(asList(
                new TopicPartition("foo", 0)
        ));

        // then
        verify(consumer).seek(new TopicPartition("foo", 0), 4711);
    }

    @Test
    public void shouldRemoveShardsAfterPartitionRevoked() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final Consumer<String, String> consumer = mock(Consumer.class);

        final ConsumerRebalanceHandler handler = new ConsumerRebalanceHandler(
                "foo",
                ChannelPosition.fromHorizon(),
                eventPublisher,
                consumer);

        handler.onPartitionsAssigned(asList(
                new TopicPartition("foo", 0),
                new TopicPartition("foo", 1)
        ));

        // when
        handler.onPartitionsRevoked(asList(
                new TopicPartition("foo", 1)
        ));

        // then
        assertThat(handler.shardsAssignedAndPositioned(), is(true));
        assertThat(handler.getCurrentPartitions(), contains("0"));
    }

    @Test
    public void shouldRemoveShardsAfterPartitionLost() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final Consumer<String, String> consumer = mock(Consumer.class);

        final ConsumerRebalanceHandler handler = new ConsumerRebalanceHandler(
                "foo",
                ChannelPosition.fromHorizon(),
                eventPublisher,
                consumer);

        handler.onPartitionsAssigned(asList(
                new TopicPartition("foo", 0),
                new TopicPartition("foo", 1)
        ));

        // when
        handler.onPartitionsLost(asList(
                new TopicPartition("foo", 1)
        ));

        // then
        assertThat(handler.shardsAssignedAndPositioned(), is(true));
        assertThat(handler.getCurrentPartitions(), contains("0"));
    }

}
