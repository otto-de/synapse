package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.status.indicator.StatusDetailIndicator;
import de.otto.synapse.eventsource.EventSourceNotification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

@Component
public class EventSourcingStatusDetailIndicator implements StatusDetailIndicator {

    private SortedMap<String, Long> startingTimeMap = new TreeMap<>();

    private SortedMap<String, StatusDetail> statusDetailMap = new TreeMap<>();
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
                startingTimeMap.put(eventSourceAndChannelName(eventSourceNotification), clock.millis());
                break;
            case FINISHED:
                long runtime = clock.millis() - startingTimeMap.get(eventSourceAndChannelName(eventSourceNotification));
                statusDetail = createStatusDetail(Status.OK, channelName, String.format("%s Finished consumption after %d seconds.", eventSourceNotification.getMessage(), runtime / 1000));
                break;
        }

        statusDetailMap.put(channelName, statusDetail);

    }

    private String eventSourceAndChannelName(EventSourceNotification eventSourceNotification) {
        return eventSourceNotification.getEventSourceName() + ":" + eventSourceNotification.getChannelName();
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
