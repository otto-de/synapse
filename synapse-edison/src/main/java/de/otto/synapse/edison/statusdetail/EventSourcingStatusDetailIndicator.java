package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.status.indicator.StatusDetailIndicator;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.eventsource.EventSourceNotification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static java.lang.String.format;
import static java.time.Instant.now;

@Component
public class EventSourcingStatusDetailIndicator implements StatusDetailIndicator {

    private SortedMap<String, StatusDetail> statusDetailMap = new TreeMap<>();
    private Map<String, ChannelPosition> mapShardIdToDurationBehind = new ConcurrentHashMap<>();
    private Map<String, Instant> channelStartupTimes = new ConcurrentHashMap<>();
    private Clock clock;

    @Autowired
    public EventSourcingStatusDetailIndicator() {
        clock = Clock.systemDefaultZone();
    }

    //for test
    EventSourcingStatusDetailIndicator(Clock clock) {
        this.clock = clock;
    }

    @EventListener
    public void onEventSourceNotification(EventSourceNotification eventSourceNotification) {
        String channelName = eventSourceNotification.getChannelName();
        StatusDetail statusDetail = createStatusDetail(Status.WARNING, channelName, "Should not happen.");

        switch (eventSourceNotification.getStatus()) {
            case FAILED:
                statusDetail = createStatusDetail(Status.ERROR, channelName, eventSourceNotification.getMessage());
                break;
            case STARTED:
                statusDetail = createStatusDetail(Status.OK, channelName, eventSourceNotification.getMessage());
                channelStartupTimes.put(channelName, clock.instant());
                break;
            case RUNNING:
                final ChannelPosition channelPosition = eventSourceNotification.getChannelPosition() != null
                        ? eventSourceNotification.getChannelPosition()
                        : fromHorizon();
                if (!channelPosition.shards().isEmpty()) {
                    for (final String shardId : channelPosition.shards()) {
                        ChannelPosition previousChannelPosition = mapShardIdToDurationBehind.getOrDefault(shardId, fromHorizon());
                        ChannelPosition mergedChannelPosition = ChannelPosition.merge(previousChannelPosition, channelPosition);
                        mapShardIdToDurationBehind.put(shardId, mergedChannelPosition);
                        statusDetail = createStatusDetail(Status.OK, channelName, format("Channel is %s behind head.", mergedChannelPosition.getDurationBehind()));
                    }
                } else {
                    statusDetail = createStatusDetail(Status.OK, channelName, "Unknown duration behind head");
                }
                break;
            case FINISHED:
                final Duration runtime = Duration.between(channelStartupTimes.get(channelName), clock.instant());
                statusDetail = createStatusDetail(Status.OK, channelName, format("%s Finished consumption after %s.", eventSourceNotification.getMessage(), runtime));
                break;
        }

        statusDetailMap.put(channelName, statusDetail);

    }

    @Override
    public StatusDetail statusDetail() {
        return null;
    }

    @Override
    public List<StatusDetail> statusDetails() {
        return new ArrayList<>(statusDetailMap.values());
    }

    private StatusDetail createStatusDetail(Status status, String name, String message) {
        return StatusDetail.statusDetail(name, status, message);
    }

}
