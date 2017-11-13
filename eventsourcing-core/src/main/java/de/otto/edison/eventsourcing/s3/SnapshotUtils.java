package de.otto.edison.eventsourcing.s3;

public class SnapshotUtils {

    public static final String COMPACTION_FILE_EXTENSION = ".json.zip";

    public static String createBucketName(String streamName, String snapshotBucketTemplate) {
        return snapshotBucketTemplate.replace("{stream-name}", streamName);
    }

    public static String getSnapshotFileNamePrefix(String streamName) {
        return String.format("compaction-%s-snapshot-", streamName);
    }

}
