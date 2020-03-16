package de.otto.synapse.edison.metrics;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.edison.health.StartupHealthIndicator;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;

import java.time.Duration;
import java.util.Optional;

import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.edison.metrics.ChannelMetricsReporter.GAUGE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChannelMetricsReporterTest {

    private EventSource eventSource2;
    private EventSource eventSource1;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        eventSource1 = mock(EventSource.class);
        when(eventSource1.getChannelName()).thenReturn("mychannel1");
        eventSource2 = mock(EventSource.class);
        when(eventSource2.getChannelName()).thenReturn("mychannel2");

    }

    @Test
    void shouldRegisterGaugeForEveryChannel() {

        //when
        new ChannelMetricsReporter(meterRegistry, Optional.of(ImmutableList.of(eventSource1, eventSource2)), Optional.of(new StartupHealthIndicator(Optional.of(ImmutableList.of(eventSource1, eventSource2)))));

        //then
        assertThat(meterRegistry.get(GAUGE_NAME).tag("channel", "mychannel1").gauge(), notNullValue());
        assertThat(meterRegistry.get(GAUGE_NAME).tag("channel", "mychannel2").gauge(), notNullValue());

        assertThat(meterRegistry.find(GAUGE_NAME).tag("channel", "mychannel3").gauge(), nullValue());
    }

    @Test
    void shouldRegisterGaugeIfReceivedMessageContainsAnUnknownChannel() {

        //given
        ChannelMetricsReporter metricsReporter = new ChannelMetricsReporter(meterRegistry, Optional.of(ImmutableList.of(eventSource1)), Optional.empty());

        //when
        metricsReporter.messageReceived(
                MessageReceiverNotification.builder()
                        .withChannelName("unknownChannel")
                        .withChannelDurationBehind(channelDurationBehind()
                                .with("shard1", Duration.ofSeconds(10))
                                .build())
                        .withStatus(MessageReceiverStatus.RUNNING)
                        .withMessage("message")
                        .build());

        //then
        assertThat(meterRegistry.get(GAUGE_NAME).tag("channel", "unknownChannel").gauge().measure().iterator().next().getValue(), is(10000.0));
    }

    @Test
    void shouldSendDurationBehindMetricWhenHealthy() {
        StartupHealthIndicator startupHealthIndicator = mock(StartupHealthIndicator.class);
        when(startupHealthIndicator.health()).thenReturn(Health.up().build());
        ChannelMetricsReporter metricsReporter = new ChannelMetricsReporter(meterRegistry, Optional.of(ImmutableList.of(eventSource1, eventSource2)), Optional.of(startupHealthIndicator));

        //when
        metricsReporter.messageReceived(
                MessageReceiverNotification.builder()
                        .withChannelName("mychannel1")
                        .withChannelDurationBehind(channelDurationBehind()
                                .with("shard1", Duration.ofSeconds(10))
                                .build())
                        .withStatus(MessageReceiverStatus.RUNNING)
                        .withMessage("message")
                        .build());

        //then
        assertThat(meterRegistry.get(GAUGE_NAME).tag("channel", "mychannel1").gauge().measure().iterator().next().getValue(), is(10000.0));
        assertThat(meterRegistry.get(GAUGE_NAME).tag("channel", "mychannel2").gauge().measure().iterator().next().getValue(), is(0.0));
    }

    @Test
    void shouldSendDurationBehindMetricWhenHealthUnavailable() {
        ChannelMetricsReporter metricsReporter = new ChannelMetricsReporter(meterRegistry, Optional.of(ImmutableList.of(eventSource1, eventSource2)), Optional.empty());

        //when
        metricsReporter.messageReceived(
                MessageReceiverNotification.builder()
                        .withChannelName("mychannel1")
                        .withChannelDurationBehind(channelDurationBehind()
                                .with("shard1", Duration.ofSeconds(10))
                                .build())
                        .withStatus(MessageReceiverStatus.RUNNING)
                        .withMessage("message")
                        .build());

        //then
        assertThat(meterRegistry.get(GAUGE_NAME).tag("channel", "mychannel1").gauge().measure().iterator().next().getValue(), is(10000.0));
        assertThat(meterRegistry.get(GAUGE_NAME).tag("channel", "mychannel2").gauge().measure().iterator().next().getValue(), is(0.0));
    }

    @Test
    void shouldNotSendDurationBehindMetricWhenUnhealthy() {
        StartupHealthIndicator startupHealthIndicator = mock(StartupHealthIndicator.class);
        when(startupHealthIndicator.health()).thenReturn(Health.down().build());
        ChannelMetricsReporter metricsReporter = new ChannelMetricsReporter(meterRegistry, Optional.of(ImmutableList.of(eventSource1, eventSource2)), Optional.of(startupHealthIndicator));

        //when
        metricsReporter.messageReceived(
                MessageReceiverNotification.builder()
                        .withChannelName("mychannel1")
                        .withChannelDurationBehind(channelDurationBehind()
                                .with("shard1", Duration.ofSeconds(10))
                                .build())
                        .withStatus(MessageReceiverStatus.RUNNING)
                        .withMessage("message")
                        .build());

        //then
        assertThat(meterRegistry.get(GAUGE_NAME).tag("channel", "mychannel1").gauge().measure().iterator().next().getValue(), is(0.0));
        assertThat(meterRegistry.get(GAUGE_NAME).tag("channel", "mychannel2").gauge().measure().iterator().next().getValue(), is(0.0));
    }
}

