package de.otto.edison.eventsourcing.aws.s3;

import de.otto.edison.eventsourcing.aws.configuration.SnapshotProperties;

public class SnapshotServiceTestUtils {
    public static SnapshotProperties snapshotProperties() {
        SnapshotProperties properties = new SnapshotProperties();
        properties.setBucketName("test-teststream");
        return properties;
    }
}
