package de.otto.synapse.edison.logging;

import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.slf4j.LoggerFactory.getLogger;

@Component
public class EventSourceSyncDurationLogger {

    private static final Logger LOG = getLogger(EventSourceSyncDurationLogger.class);
    private static final long TEN_SECONDS = 10000;
    private final Set<String> allChannels;
    private final Set<String> healthyChannels;

    private final AtomicBoolean startupDone = new AtomicBoolean(false);

    private Clock clock = Clock.systemDefaultZone();
    private Instant startTime;

    private final Map<String, Instant> mapChannelToStartTime = new ConcurrentHashMap<>();

    void setClock(Clock clock) {
        this.clock = clock;
    }

    @Autowired
    public EventSourceSyncDurationLogger(final Optional<List<EventSource>> eventSources) {
        allChannels = eventSources.orElse(Collections.emptyList())
                .stream()
                .map(EventSource::getChannelName)
                .collect(toSet());
        healthyChannels = ConcurrentHashMap.newKeySet();
    }

    @EventListener
    public void on(final MessageReceiverNotification notification) {
        if (startupDone.get()) {
            return;
        }

        String channelName = notification.getChannelName();
        if (notification.getStatus() == MessageReceiverStatus.STARTING) {
            mapChannelToStartTime.put(channelName, clock.instant());
            if (startTime == null) {
                startTime = clock.instant();
            }
        } else if (notification.getStatus() == MessageReceiverStatus.RUNNING &&
                mapChannelToStartTime.containsKey(channelName) &&
                isInSync(notification.getChannelDurationBehind())) {

            Instant stopTime = clock.instant();
            Duration duration = Duration.between(mapChannelToStartTime.get(channelName), stopTime);

            log(format("KinesisEventSource '%s' duration for getting in sync: %s", channelName, duration.toString()));

            healthyChannels.add(channelName);
            if (allChannelsAreUpToDate()) {
                log(format("All channels up to date after %s", Duration.between(startTime, stopTime)));
                startupDone.set(true);
            }

            mapChannelToStartTime.remove(channelName);
        }
    }

    private boolean allChannelsAreUpToDate() {
        return healthyChannels.containsAll(allChannels);
    }

    private boolean isInSync(Optional<ChannelDurationBehind> channelDurationBehind) {
        return channelDurationBehind.isPresent() &&
                channelDurationBehind.get().getDurationBehind().toMillis() <= TEN_SECONDS;
    }

    void log(String message) {
        LOG.info(message);
    }

}
