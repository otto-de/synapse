package de.otto.synapse.aws.s3;

import de.otto.synapse.configuration.aws.SnapshotProperties;

public class SnapshotServiceTestUtils {
    public static SnapshotProperties snapshotProperties() {
        SnapshotProperties properties = new SnapshotProperties();
        properties.setBucketName("test-teststream");
        return properties;
    }
}
