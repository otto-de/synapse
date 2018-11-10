package de.otto.synapse.compaction.s3;

import de.otto.synapse.configuration.aws.SnapshotProperties;
import de.otto.synapse.helper.s3.S3Helper;
import org.slf4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import static de.otto.synapse.compaction.s3.SnapshotFileHelper.*;
import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;
import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotReadService {

    private static final Logger LOG = getLogger(SnapshotReadService.class);

    private final S3Helper s3Helper;
    private final String snapshotBucketName;

    private File forcedSnapshotFile = null;

    public SnapshotReadService(final SnapshotProperties properties,
                               final S3Client s3Client) {
        this.s3Helper = new S3Helper(s3Client);
        this.snapshotBucketName = properties.getBucketName();
    }

    /**
     * Force to read a local snapshot file instead of retrieving it from S3 bucket.
     *
     * @param file local snapshot file to read
     */
    public void setSnapshotFile(File file) {
        Objects.requireNonNull(file, "file must not be null");
        if (!file.exists() || !file.canRead()) {
            throw new IllegalArgumentException("snapshot file does not exists or is not readable");
        }
        this.forcedSnapshotFile = file;
    }

    public Optional<File> retrieveLatestSnapshot(String channelName) {
        if (forcedSnapshotFile != null) {
            LOG.info("Use local Snapshot file: {}", forcedSnapshotFile);
            return Optional.of(forcedSnapshotFile);
        }

        LOG.info("Start downloading snapshot from S3");
        logDiskUsage();

        Optional<File> latestSnapshot = getLatestSnapshot(channelName);
        if (latestSnapshot.isPresent()) {
            LOG.info("Finished downloading snapshot {}", latestSnapshot.get().getName());
            logDiskUsage();
        } else {
            LOG.warn("No snapshot found.");
        }
        return latestSnapshot;
    }

    Optional<File> getLatestSnapshot(final String channelName) {
        Optional<S3Object> s3Object = fetchSnapshotMetadataFromS3(snapshotBucketName, channelName);
        if (s3Object.isPresent()) {
            String latestSnapshotKey = s3Object.get().key();
            Path snapshotFile = getTempFile(latestSnapshotKey);

            if (existsAndHasSize(snapshotFile, s3Object.get().size())) {
                LOG.info("Snapshot on disk is same as in S3, keep it and use it: {}", snapshotFile.toAbsolutePath().toString());
                return Optional.of(snapshotFile.toFile());
            }

            removeTempFiles("*-snapshot-*.json.zip");
            LOG.info("Downloading snapshot file to {}", snapshotFile.getFileName().toAbsolutePath().toString());
            if (s3Helper.download(snapshotBucketName, latestSnapshotKey, snapshotFile)) {
                return Optional.of(snapshotFile.toFile());
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }


    Optional<S3Object> fetchSnapshotMetadataFromS3(String bucketName, String channelName) {
        return s3Helper.listAll(bucketName)
                .stream()
                .filter(o -> o.key().startsWith(getSnapshotFileNamePrefix(channelName)))
                .filter(o -> o.key().endsWith(COMPACTION_FILE_EXTENSION))
                .min(comparing(S3Object::lastModified, reverseOrder()));
    }

}
