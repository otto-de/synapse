package de.otto.synapse.edison.logging;

import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
import org.slf4j.Logger;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class EventSourceSyncDurationLogger {

    private static final Logger LOG = getLogger(EventSourceSyncDurationLogger.class);
    private static final long TEN_SECONDS = 10000;

    private Clock clock = Clock.systemDefaultZone();
    private final Map<String, Instant> mapChannelToStartTime = new ConcurrentHashMap<>();

    void setClock(Clock clock) {
        this.clock = clock;
    }

    @EventListener
    public String on(final MessageReceiverNotification notification) {
        String channelName = notification.getChannelName();
        String logMessage = "";
        if (notification.getStatus() == MessageReceiverStatus.STARTING) {
            mapChannelToStartTime.put(channelName, clock.instant());
        } else if (notification.getStatus() == MessageReceiverStatus.RUNNING &&
                mapChannelToStartTime.containsKey(channelName) &&
                isInSync(notification.getChannelDurationBehind())) {
            Instant stopTime = clock.instant();
            Duration duration = Duration.between(mapChannelToStartTime.get(channelName), stopTime);
            logMessage = String.format("KinesisEventSource '%s' duration for getting in sync : %s", channelName, duration.toString());
            LOG.info(logMessage);
            mapChannelToStartTime.remove(channelName);
        }
        return logMessage;
    }

    private boolean isInSync(Optional<ChannelDurationBehind> channelDurationBehind) {
        return channelDurationBehind.isPresent() &&
                channelDurationBehind.get().getDurationBehind().toMillis() <= TEN_SECONDS;
    }

}
