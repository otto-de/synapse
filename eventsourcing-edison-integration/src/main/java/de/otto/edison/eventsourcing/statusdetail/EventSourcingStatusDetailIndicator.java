package de.otto.edison.eventsourcing.statusdetail;

import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.status.indicator.StatusDetailIndicator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class EventSourcingStatusDetailIndicator implements StatusDetailIndicator {

    private Map<String, Long> startingTimeMap = new HashMap<>();

    private Map<String, StatusDetail> statusDetailMap = new HashMap<>();
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
        String streamName = eventSourceNotification.getStreamName();
        StatusDetail statusDetail = createStatusDetail(Status.WARNING, streamName, "Should not happen.");

        switch (eventSourceNotification.getStatus()) {
            case FAILED:
                statusDetail = createStatusDetail(Status.ERROR, streamName, eventSourceNotification.getMessage());
                break;
            case STARTED:
                statusDetail = createStatusDetail(Status.OK, streamName, eventSourceNotification.getMessage());
                startingTimeMap.put(eventSourceAndStreamName(eventSourceNotification), clock.millis());
                break;
            case FINISHED:
                long runtime = clock.millis() - startingTimeMap.get(eventSourceAndStreamName(eventSourceNotification));
                statusDetail = createStatusDetail(Status.OK, streamName, String.format("%s Finished consumption after %d seconds.", eventSourceNotification.getMessage(), runtime / 1000));
                break;
        }

        statusDetailMap.put(streamName, statusDetail);

    }

    private String eventSourceAndStreamName(EventSourceNotification eventSourceNotification) {
        return eventSourceNotification.getEventSourceName() + ":" + eventSourceNotification.getStreamName();
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
