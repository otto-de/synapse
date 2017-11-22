package de.otto.edison.eventsourcing.s3;

import de.otto.edison.aws.s3.S3Service;
import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;
import org.slf4j.Logger;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

import static com.google.common.base.StandardSystemProperty.JAVA_IO_TMPDIR;
import static de.otto.edison.eventsourcing.s3.SnapshotUtils.*;
import static java.lang.String.format;
import static java.nio.file.Files.delete;
import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;
import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotReadService {

    private static final Logger LOG = getLogger(SnapshotReadService.class);
    private static final int ONE_MB = 1024 * 1024;
    private static final long MAX_SNAPSHOT_FILE_AGE = 1000 * 60 * 60 * 24 * 3; //3 days //TODO make configurable

    private S3Service s3Service;
    private String snapshotBucketName;


    public SnapshotReadService(final S3Service s3Service,
                               final EventSourcingProperties properties) {
        this.s3Service = s3Service;
        snapshotBucketName = properties.getSnapshot().getBucketName();
    }


    public Optional<File> downloadLatestSnapshot(SnapshotEventSource snapshotEventSource) {
        LOG.info("Start downloading snapshot from S3");
        infoDiskUsage();

        Optional<File> latestSnapshot = getLatestSnapshotFromBucket(snapshotEventSource.getStreamName());
        if (latestSnapshot.isPresent()) {
            LOG.info("Finished downloading snapshot {}", latestSnapshot.get().getName());
            infoDiskUsage();
        } else {
            LOG.warn("No snapshot found.");
        }
        return latestSnapshot;
    }

    Optional<File> getLatestSnapshotFromBucket(final String streamName) {
        Optional<S3Object> s3Object = getLatestZip(snapshotBucketName, streamName);
        if (s3Object.isPresent()) {
            String latestSnapshotKey = s3Object.get().key();
            Path snapshotFile = Paths.get(getTempDir() + "/" + latestSnapshotKey);

            if (snapshotFile.toFile().length() == s3Object.get().size()) {
                LOG.info("Snapshot on disk is same as in S3, keep it and use it: {}", snapshotFile.toAbsolutePath().toString());
                return Optional.of(snapshotFile.toFile());
            } else {
                Optional<File> recentSnapshot = findRecentLocalSnapshot(streamName);
                if (recentSnapshot.isPresent()) {
                    LOG.info("Snapshot on disk is not too old, keep it and use it: {}", recentSnapshot.get().toPath().toAbsolutePath().toString());
                    return recentSnapshot;
                }
            }


            LOG.info("Downloading snapshot file to {}", snapshotFile.getFileName().toAbsolutePath().toString());
            if (s3Service.download(snapshotBucketName, latestSnapshotKey, snapshotFile)) {
                return Optional.of(snapshotFile.toFile());
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }


    private Optional<File> findRecentLocalSnapshot(String streamName) {
        String snapshotFileNamePrefix = getSnapshotFileNamePrefix(streamName);
        String snapshotFileSuffix = ".json.zip";
        Optional<File> newestFile;
        try {
            newestFile = Files.find(Paths.get(getTempDir()), 1,
                    (path, basicFileAttributes) -> (path.getFileName().toString().startsWith(snapshotFileNamePrefix) && path.getFileName().toString().endsWith(snapshotFileSuffix)))
                    .filter(this::isValid)
                    .max((path1, path2) -> (int) (path1.toFile().lastModified() - path2.toFile().lastModified()))
                    .filter(path -> System.currentTimeMillis() - path.toFile().lastModified() < MAX_SNAPSHOT_FILE_AGE)
                    .map(Path::toFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return newestFile;
    }

    private String getTempDir() {
        return System.getProperty(JAVA_IO_TMPDIR.key());
    }

    @SuppressWarnings("try")
    private boolean isValid(Path path) {
        try (ZipFile ignored = new ZipFile(path.toFile())) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    Optional<S3Object> getLatestZip(String bucketName, String streamName) {
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
                    delete(file.toPath());
                }
            } catch (IOException e) {
                LOG.error("Error deleting temp file while calculating disk usage:" + e.getMessage());
            }
        }
    }

    public void deleteOlderSnapshots(String streamName) {
        String snapshotFileNamePrefix = getSnapshotFileNamePrefix(streamName);
        String snapshotFileSuffix = ".json.zip";
        List<File> oldestFiles;
        try {
            oldestFiles = Files.find(Paths.get(getTempDir()), 1,
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
