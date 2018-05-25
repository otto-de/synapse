package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.status.indicator.StatusDetailIndicator;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.SnapshotReaderNotification;
import de.otto.synapse.info.SnapshotReaderStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toList;

@Component
public class SnapshotStatusDetailIndicator implements StatusDetailIndicator {

    private final Map<String, StatusDetail> statusDetails;

    @Autowired
    public SnapshotStatusDetailIndicator(final Optional<List<EventSource>> eventSources) {
        statusDetails = eventSources.orElse(emptyList())
                .stream()
                .map(EventSource::getChannelName)
                .map(channelName -> StatusDetail.statusDetail(channelName, Status.OK, "No snapshot info available."))
                .collect(toConcurrentMap(
                        StatusDetail::getName,
                        identity())
                );
    }

    @EventListener
    public void on(final SnapshotReaderNotification notification) {
        final String channelName = notification.getChannelName();
        if (notification.getStatus() == SnapshotReaderStatus.FAILED) {
            statusDetails.put(
                    channelName,
                    StatusDetail.statusDetail(channelName, Status.ERROR, notification.getMessage()));
        } else {
            statusDetails.put(channelName, StatusDetail.statusDetail(channelName, Status.OK, notification.getMessage()));
        }
    }
    @Override
    public StatusDetail statusDetail() {
        return null;
    }

    @Override
    public List<StatusDetail> statusDetails() {
        return statusDetails.values()
                .stream()
                .sorted(comparing(StatusDetail::getName))
                .collect(toList());
    }

}
