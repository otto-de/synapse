package de.otto.edison.eventsourcing.s3;

import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;

public class SnapshotServiceTestUtils {
    public static EventSourcingProperties createEventSourcingProperties() {
        EventSourcingProperties.Snapshot snapshot = new EventSourcingProperties.Snapshot();
        snapshot.setBucketName("test-teststream");

        EventSourcingProperties eventSourcingProperties = new EventSourcingProperties();
        eventSourcingProperties.setSnapshot(snapshot);
        return eventSourcingProperties;
    }
}
