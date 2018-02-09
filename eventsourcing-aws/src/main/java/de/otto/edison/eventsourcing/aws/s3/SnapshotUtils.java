package de.otto.edison.eventsourcing.aws.s3;

public class SnapshotUtils {

    public static final String COMPACTION_FILE_EXTENSION = ".json.zip";

    public static String getSnapshotFileNamePrefix(String streamName) {
        return String.format("compaction-%s-snapshot-", streamName);
    }

}
