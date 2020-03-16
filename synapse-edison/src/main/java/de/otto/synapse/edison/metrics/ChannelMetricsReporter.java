package de.otto.synapse.edison.metrics;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.edison.health.StartupHealthIndicator;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageReceiverNotification;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.slf4j.Logger;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.slf4j.LoggerFactory.getLogger;

@ConditionalOnProperty(
        prefix = "synapse.edison.metrics",
        name = "enabled",
        havingValue = "true")
@Component
public class ChannelMetricsReporter {

    private static final Logger LOG = getLogger(ChannelMetricsReporter.class);
    static final String GAUGE_NAME = "kinesis_duration_behind_gauge";
    private final ConcurrentMap<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final Optional<StartupHealthIndicator> startupHealthIndicator;
    private final MeterRegistry meterRegistry;

    public ChannelMetricsReporter(final MeterRegistry meterRegistry,
                                  final Optional<List<EventSource>> eventSources,
                                  final Optional<StartupHealthIndicator> startupHealthIndicator) {
        this.meterRegistry = meterRegistry;
        eventSources.ifPresent(sources -> sources.forEach(eventSource -> this.createChannelNameGauge(eventSource.getChannelName())));
        this.startupHealthIndicator = startupHealthIndicator;
    }

    @EventListener
    public void messageReceived(MessageReceiverNotification messageReceiverNotification) {
        String channelName = messageReceiverNotification.getChannelName();
        AtomicLong atomicLong = gauges.get(channelName);

        if (isHealthyOrHealthUnavailable() && messageReceiverNotification.getChannelDurationBehind().isPresent()) {
            if (atomicLong == null) {
                LOG.warn("gauge for {} could not be found, this is likely due to a programmatically configured eventsource. Recreating gauge", channelName);
                createChannelNameGauge(channelName);
                atomicLong = gauges.get(channelName);
            }
            atomicLong.set(messageReceiverNotification.getChannelDurationBehind().get().getDurationBehind().toMillis());
        }
    }

    private void createChannelNameGauge(String channelName) {
        AtomicLong channel = meterRegistry.gauge(GAUGE_NAME, ImmutableList.of(Tag.of("channel", channelName)), new AtomicLong(0));
        gauges.put(channelName, channel);
    }

    private boolean isHealthyOrHealthUnavailable() {
        return !startupHealthIndicator.isPresent() || startupHealthIndicator.get().health().getStatus().equals(Status.UP);
    }
}

