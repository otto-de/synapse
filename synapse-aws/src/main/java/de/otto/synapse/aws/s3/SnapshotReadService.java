package de.otto.synapse.aws.s3;

import de.otto.edison.aws.s3.S3Service;
import de.otto.synapse.aws.configuration.SnapshotProperties;
import org.slf4j.Logger;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static de.otto.synapse.aws.s3.SnapshotUtils.COMPACTION_FILE_EXTENSION;
import static de.otto.synapse.aws.s3.SnapshotUtils.getSnapshotFileNamePrefix;
import static java.lang.String.format;
import static java.nio.file.Files.delete;
import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;
import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotReadService {

    private static final Logger LOG = getLogger(SnapshotReadService.class);
    private static final int ONE_MB = 1024 * 1024;

    private final S3Service s3Service;
    private final String snapshotBucketName;
    private final TempFileHelper tempFileHelper;

    private File forcedSnapshotFile = null;

    public SnapshotReadService(final SnapshotProperties properties,
                               final S3Service s3Service) {
        this(properties, s3Service, new TempFileHelper());
    }

    public SnapshotReadService(final SnapshotProperties properties,
                               final S3Service s3Service,
                               final TempFileHelper tempFileHelper) {
        this.s3Service = s3Service;
        this.snapshotBucketName = properties.getBucketName();
        this.tempFileHelper = tempFileHelper;
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

    public Optional<File> retrieveLatestSnapshot(String streamName) {
        if (forcedSnapshotFile != null) {
            LOG.info("Use local Snapshot file: {}", forcedSnapshotFile);
            return Optional.of(forcedSnapshotFile);
        }

        LOG.info("Start downloading snapshot from S3");
        infoDiskUsage();

        Optional<File> latestSnapshot = getLatestSnapshot(streamName);
        if (latestSnapshot.isPresent()) {
            LOG.info("Finished downloading snapshot {}", latestSnapshot.get().getName());
            infoDiskUsage();
        } else {
            LOG.warn("No snapshot found.");
        }
        return latestSnapshot;
    }

    Optional<File> getLatestSnapshot(final String streamName) {
        Optional<S3Object> s3Object = fetchSnapshotMetadataFromS3(snapshotBucketName, streamName);
        if (s3Object.isPresent()) {
            String latestSnapshotKey = s3Object.get().key();
            Path snapshotFile = tempFileHelper.getTempFile(latestSnapshotKey);

            if (tempFileHelper.existsAndHasSize(snapshotFile, s3Object.get().size())) {
                LOG.info("Snapshot on disk is same as in S3, keep it and use it: {}", snapshotFile.toAbsolutePath().toString());
                return Optional.of(snapshotFile.toFile());
            }


            tempFileHelper.removeTempFiles("*-snapshot-*.json.zip");
            LOG.info("Downloading snapshot file to {}", snapshotFile.getFileName().toAbsolutePath().toString());
            if (s3Service.download(snapshotBucketName, latestSnapshotKey, snapshotFile)) {
                return Optional.of(snapshotFile.toFile());
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }


    Optional<S3Object> fetchSnapshotMetadataFromS3(String bucketName, String streamName) {
        return s3Service.listAll(bucketName)
                .stream()
                .filter(o -> o.key().startsWith(getSnapshotFileNamePrefix(streamName)))
                .filter(o -> o.key().endsWith(COMPACTION_FILE_EXTENSION))
                .sorted(comparing(S3Object::lastModified, reverseOrder()))
                .findFirst();
    }

    private void infoDiskUsage() {
        File file = null;
        try {
            file = File.createTempFile("tempFileForDiskUsage", ".txt");
            float usableSpace = (float) file.getUsableSpace() / 1024 / 1024 / 1024;
            float freeSpace = (float) file.getFreeSpace() / 1024 / 1024 / 1024;
            LOG.info(format("Available DiskSpace: usable %.3f GB / free %.3f GB", usableSpace, freeSpace));

            String tempDirContent = Files.list(Paths.get(System.getProperty("java.io.tmpdir")))
                    .filter(path -> path.toFile().isFile())
                    .filter(path -> path.toFile().length() > ONE_MB)
                    .map(path -> String.format("%s %dmb", path.toString(), path.toFile().length() / ONE_MB))
                    .collect(Collectors.joining("\n"));
            LOG.info("files in /tmp > 1mb: \n {}", tempDirContent);

        } catch (IOException e) {
            LOG.info("Error calculating disk usage: " + e.getMessage());
        } finally {
            try {
                if (file != null) {
                    LOG.info("delete file {}", file.toPath().toString());
                    delete(file.toPath());
                }
            } catch (IOException e) {
                LOG.error("Error deleting temp file while calculating disk usage:" + e.getMessage());
            }
        }
    }

    public void deleteOlderSnapshots(String streamName) {
        if (forcedSnapshotFile != null) {
            LOG.info("Skip deleting local Snapshot file: {}", forcedSnapshotFile);
            return;
        }

        String snapshotFileNamePrefix = getSnapshotFileNamePrefix(streamName);
        String snapshotFileSuffix = ".json.zip";
        List<File> oldestFiles;
        try {
            oldestFiles = Files.find(Paths.get(tempFileHelper.getTempDir()), 1,
                    (path, basicFileAttributes) -> (path.getFileName().toString().startsWith(snapshotFileNamePrefix) && path.getFileName().toString().endsWith(snapshotFileSuffix)))
                    .sorted((path1, path2) -> (int) (path2.toFile().lastModified() - path1.toFile().lastModified()))
                    .map(Path::toFile)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (oldestFiles.size() > 1) {
            oldestFiles.subList(1, oldestFiles.size()).forEach(this::deleteSnapshotFile);
        }
    }

    private void deleteSnapshotFile(File snapshotFile) {
        boolean success = snapshotFile.delete();
        if (success) {
            LOG.info("deleted {}", snapshotFile.getName());
        } else {
            LOG.warn("deletion of {} failed", snapshotFile.getName());
        }
    }


}
