package de.otto.edison.eventsourcing.statusdetail;

import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;
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
    public EventSourcingStatusDetailIndicator(Clock clock) {
        this.clock = clock;
    }

    @EventListener
    public void onEventSourceNotification(EventSourceNotification eventSourceNotification) {
        String streamName = eventSourceNotification.getEventSource().getStreamName();
        StatusDetail statusDetail = createStatusDetail(Status.WARNING, streamName, "Should not happen.");

        if (eventSourceNotification.getEventSource().getClass().equals(SnapshotEventSource.class)) {
            switch (eventSourceNotification.getStatus()) {
                case FAILED:
                    statusDetail = createStatusDetail(Status.ERROR, streamName, "Stopped startup process, because snapshot loading from s3 failed: " + eventSourceNotification.getMessage());
                    break;
                case STARTED:
                    statusDetail = createStatusDetail(Status.OK, streamName, "Loading snapshot");
                    startingTimeMap.put(streamName, clock.millis());
                    break;
                case FINISHED:
                    long runtime = clock.millis() - startingTimeMap.get(streamName);
                    statusDetail = createStatusDetail(Status.OK, streamName, "Startup time was " + (runtime / 1000) + " seconds.");
                    break;
            }
        }
        if (eventSourceNotification.getEventSource().getClass().equals(KinesisEventSource.class)) {
            switch (eventSourceNotification.getStatus()) {
                case FAILED:
                    statusDetail = createStatusDetail(Status.ERROR, streamName, String.format("Error consuming from kinesis: %s", eventSourceNotification.getMessage()));
                    break;
                case STARTED:
                    statusDetail = createStatusDetail(Status.OK, streamName, "Consuming from kinesis.");
                    break;
                case FINISHED:
                    statusDetail = createStatusDetail(Status.WARNING, streamName, "Consumer finished.");
            }
        }
        statusDetailMap.put(streamName, statusDetail);

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
