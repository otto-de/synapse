package de.otto.synapse.endpoint.receiver.kafka;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelDurationBehind;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;

import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.RUNNING;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ChannelDurationBehindHandlerTest {

    @Test
    public void shouldAssignPartitions() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final ChannelDurationBehindHandler handler = new ChannelDurationBehindHandler("foo", eventPublisher);

        // when
        handler.onPartitionsAssigned(asList(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1)
        ));

        // then
        final ChannelDurationBehind durationBehind = handler.getChannelDurationBehind().get();
        assertThat(durationBehind.getDurationBehind(), is(ofMillis(Long.MAX_VALUE)));
        assertThat(durationBehind.getShardDurationsBehind(), is(ImmutableMap.of(
                "0", ofMillis(Long.MAX_VALUE),
                "1", ofMillis(Long.MAX_VALUE)
                )));
    }

    @Test
    public void shouldRevokePartitions() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final ChannelDurationBehindHandler handler = new ChannelDurationBehindHandler("foo", eventPublisher);
        handler.onPartitionsAssigned(asList(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1)
        ));

        // when
        handler.onPartitionsRevoked(asList(
                new TopicPartition("topic", 1)
        ));

        // then
        final ChannelDurationBehind durationBehind = handler.getChannelDurationBehind().get();
        assertThat(durationBehind.getDurationBehind(), is(ofMillis(Long.MAX_VALUE)));
        assertThat(durationBehind.getShardDurationsBehind(), is(ImmutableMap.of(
                "0", ofMillis(Long.MAX_VALUE)
        )));
    }

    @Test
    public void shouldUpdateDurationBehind() {
        // given
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final ChannelDurationBehindHandler handler = new ChannelDurationBehindHandler("foo", eventPublisher);
        handler.onPartitionsAssigned(asList(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1)
        ));

        // when
        handler.update("1", ofMillis(42));

        // then
        final ChannelDurationBehind durationBehind = handler.getChannelDurationBehind().get();
        assertThat(durationBehind.getDurationBehind(), is(ofMillis(Long.MAX_VALUE)));
        assertThat(durationBehind.getShardDurationsBehind(), is(ImmutableMap.of(
                "0", ofMillis(Long.MAX_VALUE),
                "1", ofMillis(42)
        )));

        verify(eventPublisher).publishEvent(builder()
                .withChannelName("foo")
                .withChannelDurationBehind(
                        channelDurationBehind()
                                .with("0", ofMillis(Long.MAX_VALUE))
                                .with("1", ofMillis(42))
                                .build()
                )
                .withStatus(RUNNING)
                .withMessage("Reading from Kafka stream.")
                .build());

    }
}
