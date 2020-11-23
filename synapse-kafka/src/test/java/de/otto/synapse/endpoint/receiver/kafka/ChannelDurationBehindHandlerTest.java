package de.otto.synapse.endpoint.receiver.kafka;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.channel.ChannelPosition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.RUNNING;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class ChannelDurationBehindHandlerTest {

    final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
    final Consumer<String, String> kafkaConsumer = mock(Consumer.class);
    private final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
    final ChannelDurationBehindHandler handler = new ChannelDurationBehindHandler("foo", ChannelPosition.fromHorizon(), eventPublisher, clock, kafkaConsumer);

    @Test
    public void shouldAssignPartitions() {
        // given
        List<TopicPartition> topicPartitions = asList(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1)
        );
        initializeEndOffsetsToZero(topicPartitions);

        // when
        handler.onPartitionsAssigned(topicPartitions);

        // then
        final ChannelDurationBehind durationBehind = handler.getChannelDurationBehind();
        assertThat(durationBehind.getDurationBehind(), is(ofMillis(0)));
        assertThat(durationBehind.getShardDurationsBehind(), is(ImmutableMap.of(
                "0", ofMillis(0),
                "1", ofMillis(0)
        )));
    }

    @Test
    public void shouldRevokePartitions() {
        // given
        List<TopicPartition> topicPartitions = asList(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1)
        );
        initializeEndOffsetsToZero(topicPartitions);

        handler.onPartitionsAssigned(topicPartitions);

        // when
        handler.onPartitionsRevoked(Collections.singletonList(topicPartitions.get(1)));

        // then
        final ChannelDurationBehind durationBehind = handler.getChannelDurationBehind();
        assertThat(durationBehind.getDurationBehind(), is(ofMillis(0)));
        assertThat(durationBehind.getShardDurationsBehind(), is(ImmutableMap.of("0", ofMillis(0))));
    }

    @Test
    public void shouldSetDurationBehindWhenOffsetIsBehind() {
        // given
        TopicPartition partition0 = new TopicPartition("topic", 0);
        TopicPartition partition1 = new TopicPartition("topic", 1);
        List<TopicPartition> topicPartitions = asList(
                partition0,
                partition1
        );
        initializeEndOffsetsToZero(topicPartitions);
        handler.onPartitionsAssigned(topicPartitions);

        when(kafkaConsumer.endOffsets(Collections.singletonList(partition1))).thenReturn(ImmutableMap.of(partition1, 70L));

        // when
        handler.update(partition1, 23, clock.instant().minusSeconds(42));

        // then
        final ChannelDurationBehind durationBehind = handler.getChannelDurationBehind();

        assertThat(durationBehind.getDurationBehind(), is(ofSeconds(42)));
        assertThat(durationBehind.getShardDurationsBehind(), is(ImmutableMap.of(
                "0", ofMillis(0),
                "1", ofSeconds(42)
        )));

        verify(eventPublisher).publishEvent(builder()
                .withChannelName("foo")
                .withChannelDurationBehind(
                        channelDurationBehind()
                                .with("0", ofMillis(0))
                                .with("1", ofSeconds(42))
                                .build()
                )
                .withStatus(RUNNING)
                .withMessage("Reading from Kafka stream.")
                .build());

    }

    @Test
    public void shouldSetZeroDurationBehindWhenOffsetIsUpToDate() {
        // given
        TopicPartition partition0 = new TopicPartition("topic", 0);
        TopicPartition partition1 = new TopicPartition("topic", 1);
        List<TopicPartition> topicPartitions = asList(
                partition0,
                partition1
        );
        when(kafkaConsumer.endOffsets(topicPartitions)).thenReturn(ImmutableMap.of(partition0, 120L, partition1, 70L));
        handler.onPartitionsAssigned(topicPartitions);

        when(kafkaConsumer.endOffsets(Collections.singletonList(partition1))).thenReturn(ImmutableMap.of(partition1, 70L));

        // when
        handler.update(partition1, 69, clock.instant().minusSeconds(42));

        // then
        final ChannelDurationBehind durationBehind = handler.getChannelDurationBehind();

        assertThat(durationBehind.getDurationBehind(), is(ofMillis(Long.MAX_VALUE)));
        assertThat(durationBehind.getShardDurationsBehind(), is(ImmutableMap.of(
                "0", ofMillis(Long.MAX_VALUE),
                "1", ofSeconds(0)
        )));

        verify(eventPublisher).publishEvent(builder()
                .withChannelName("foo")
                .withChannelDurationBehind(
                        channelDurationBehind()
                                .with("0", ofMillis(Long.MAX_VALUE))
                                .with("1", ofSeconds(0))
                                .build()
                )
                .withStatus(RUNNING)
                .withMessage("Reading from Kafka stream.")
                .build());

    }

    private void initializeEndOffsetsToZero(List<TopicPartition> topicPartitions) {
        Map<TopicPartition, Long> endOffsets = topicPartitions.stream().collect(Collectors.toMap(topicPartition -> topicPartition, topicPartition -> 0L));
        when(kafkaConsumer.endOffsets(topicPartitions)).thenReturn(endOffsets);
    }
}
