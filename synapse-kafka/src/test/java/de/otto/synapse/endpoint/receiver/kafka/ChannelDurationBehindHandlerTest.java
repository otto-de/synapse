package de.otto.synapse.endpoint.receiver.kafka;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelDurationBehind;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;

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
    final KafkaConsumer<String, String> kafkaConsumer = mock(KafkaConsumer.class);
    private final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
    final ChannelDurationBehindHandler handler = new ChannelDurationBehindHandler("foo", eventPublisher, clock,  kafkaConsumer);


    @Test
    public void shouldAssignPartitions() {
        // given

        // when
        handler.onPartitionsAssigned(asList(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1)
        ));

        // then
        final ChannelDurationBehind durationBehind = handler.getChannelDurationBehind();
        assertThat(durationBehind.getDurationBehind(), is(ofMillis(Long.MAX_VALUE)));
        assertThat(durationBehind.getShardDurationsBehind(), is(ImmutableMap.of(
                "0", ofMillis(Long.MAX_VALUE),
                "1", ofMillis(Long.MAX_VALUE)
                )));
    }

    @Test
    public void shouldRevokePartitions() {
        // given
        handler.onPartitionsAssigned(asList(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1)
        ));

        // when
        handler.onPartitionsRevoked(asList(
                new TopicPartition("topic", 1)
        ));

        // then
        final ChannelDurationBehind durationBehind = handler.getChannelDurationBehind();
        assertThat(durationBehind.getDurationBehind(), is(ofMillis(Long.MAX_VALUE)));
        assertThat(durationBehind.getShardDurationsBehind(), is(ImmutableMap.of(
                "0", ofMillis(Long.MAX_VALUE)
        )));
    }

    @Test
    public void shouldSetDurationBehindWhenOffsetIsBehind() {
        // given
        TopicPartition partition0 = new TopicPartition("topic", 0);
        TopicPartition partition1 = new TopicPartition("topic", 1);
        handler.onPartitionsAssigned(asList(
                partition0,
                partition1
        ));
        when(kafkaConsumer.endOffsets(Collections.singletonList(partition1))).thenReturn(ImmutableMap.of(partition1, 70L));

        // when
        handler.update(partition1, 23, clock.instant().minusSeconds(42));

        // then
        final ChannelDurationBehind durationBehind = handler.getChannelDurationBehind();

        assertThat(durationBehind.getDurationBehind(), is(ofMillis(Long.MAX_VALUE)));
        assertThat(durationBehind.getShardDurationsBehind(), is(ImmutableMap.of(
                "0", ofMillis(Long.MAX_VALUE),
                "1", ofSeconds(42)
        )));

        verify(eventPublisher).publishEvent(builder()
                .withChannelName("foo")
                .withChannelDurationBehind(
                        channelDurationBehind()
                                .with("0", ofMillis(Long.MAX_VALUE))
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
        handler.onPartitionsAssigned(asList(
                partition0,
                partition1
        ));
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
}
