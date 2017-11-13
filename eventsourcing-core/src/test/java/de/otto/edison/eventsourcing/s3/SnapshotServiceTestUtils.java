package de.otto.edison.eventsourcing.s3;

import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;

public class SnapshotServiceTestUtils {
    public static EventSourcingProperties createEventSourcingProperties() {
        EventSourcingProperties eventSourcingProperties = new EventSourcingProperties();
        EventSourcingProperties.Snapshot snapshot = new EventSourcingProperties.Snapshot();
        snapshot.setBucketTemplate("test-{stream-name}");
        eventSourcingProperties.setSnapshot(snapshot);
        return eventSourcingProperties;
    }
}
