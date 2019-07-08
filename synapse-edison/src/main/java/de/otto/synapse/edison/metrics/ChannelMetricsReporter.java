package de.otto.synapse.edison.metrics;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.edison.health.StartupHealthIndicator;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageReceiverNotification;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@ConditionalOnProperty(
        prefix = "synapse.edison.metrics",
        name = "enabled",
        havingValue = "true")
@Component
public class ChannelMetricsReporter {

    static final String GAUGE_NAME = "kinesis_duration_behind_gauge";
    private final ConcurrentMap<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final Optional<StartupHealthIndicator> startupHealthIndicator;

    public ChannelMetricsReporter(final MeterRegistry meterRegistry,
                                  final Optional<List<EventSource>> eventSources,
                                  final Optional<StartupHealthIndicator> startupHealthIndicator) {
        eventSources.ifPresent(sources -> sources.forEach(eventSource -> {
            AtomicLong channel = meterRegistry.gauge(GAUGE_NAME, ImmutableList.of(Tag.of("channel", eventSource.getChannelName())), new AtomicLong(0));
            gauges.put(eventSource.getChannelName(), channel);
        }));
        this.startupHealthIndicator = startupHealthIndicator;
    }

    @EventListener
    public void messageReceived(MessageReceiverNotification messageReceiverNotification) {
        AtomicLong atomicLong = gauges.get(messageReceiverNotification.getChannelName());

        if (isHealthyOrHealthUnavailable() && messageReceiverNotification.getChannelDurationBehind().isPresent()) {
            atomicLong.set(messageReceiverNotification.getChannelDurationBehind().get().getDurationBehind().toMillis());
        }
    }

    private boolean isHealthyOrHealthUnavailable() {
        return !startupHealthIndicator.isPresent() || startupHealthIndicator.get().health().getStatus().equals(Status.UP);
    }
}

