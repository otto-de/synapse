package de.otto.synapse.compaction.aws;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.configuration.aws.SnapshotProperties;
import de.otto.synapse.helper.s3.S3Helper;
import de.otto.synapse.logging.ProgressLogger;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static de.otto.synapse.compaction.aws.SnapshotFileHelper.*;
import static java.time.format.DateTimeFormatter.ofPattern;
import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotWriteService {
    private static final Logger LOG = getLogger(SnapshotWriteService.class);

    private static final DateTimeFormatter dateTimeFormatter = ofPattern("yyyy-MM-dd'T'HH-mmX").withZone(ZoneOffset.UTC);

    private static final String ZIP_ENTRY = "data";
    //JSON Fields
    private static final String DATA_FIELD_NAME = "data";
    private static final String START_SEQUENCE_NUMBERS_FIELD_NAME = "startSequenceNumbers";
    private static final String SHARD_FIELD_NAME = "shard";
    private static final String SEQUENCE_NUMBER_FIELD_NAME = "sequenceNumber";
    private static final int NUM_SNAPSHOTS_TO_KEEP = 6;

    private final S3Helper s3Helper;
    private final String snapshotBucketName;
    private final JsonFactory jsonFactory = new JsonFactory();

    public SnapshotWriteService(final S3Client s3Client,
                                final SnapshotProperties properties) {
        this.s3Helper = new S3Helper(s3Client);
        this.snapshotBucketName = properties.getBucketName();
    }


    public String writeSnapshot(final String channelName,
                                final ChannelPosition position,
                                final StateRepository<String> stateRepository) throws IOException {
        File snapshotFile = null;
        try {
            LOG.info("Start creating new snapshot");
            snapshotFile = createSnapshot(channelName, position, stateRepository);
            LOG.info("Finished creating snapshot file: {}", snapshotFile.getAbsolutePath());
            uploadSnapshot(this.snapshotBucketName, snapshotFile);
            LOG.info("Finished uploading snapshot file to s3");
            deleteOlderSnapshots(channelName);
        } finally {
            if (snapshotFile != null) {
                LOG.info("delete file {}", snapshotFile.toPath().toString());
                deleteFile(snapshotFile);
            }
        }
        return snapshotFile.getName();
    }

    @VisibleForTesting
    File createSnapshot(final String channelName,
                        final ChannelPosition currentChannelPosition,
                        final StateRepository<String> stateRepository) throws IOException {
        File snapshotFile = createSnapshotFile(channelName);

        try (FileOutputStream fos = new FileOutputStream(snapshotFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             ZipOutputStream zipOutputStream = new ZipOutputStream(bos)
        ) {
            ZipEntry zipEntry = new ZipEntry(ZIP_ENTRY);
            zipEntry.setMethod(ZipEntry.DEFLATED);
            zipOutputStream.putNextEntry(zipEntry);
            JsonGenerator jGenerator = jsonFactory.createGenerator(zipOutputStream, JsonEncoding.UTF8);
            jGenerator.writeStartObject();
            writeSequenceNumbers(currentChannelPosition, jGenerator);
            // write to data file
            jGenerator.writeArrayFieldStart(DATA_FIELD_NAME);

            ProgressLogger processedLogger = new ProgressLogger(LOG, stateRepository.size());
            stateRepository.keySet().forEach((key) -> {
                try {
                    processedLogger.incrementAndLog();
                    String entry = stateRepository.get(key).get();
                    if (!("".equals(entry))) {
                        jGenerator.writeStartObject();
                        jGenerator.writeStringField(key, entry);
                        jGenerator.writeEndObject();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            jGenerator.writeEndArray();
            jGenerator.writeEndObject();
            jGenerator.flush();
            zipOutputStream.closeEntry();
        } catch (Exception e) {
            LOG.info("delete file {}", snapshotFile.toPath().toString());
            deleteFile(snapshotFile);
            throw e;
        } finally {
            System.gc();
        }
        return snapshotFile;
    }

    public void deleteOlderSnapshots(final String channelName) {
        String snapshotFileNamePrefix = getSnapshotFileNamePrefix(channelName);
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
        if (oldestFiles.size() > NUM_SNAPSHOTS_TO_KEEP) {
            oldestFiles.subList(NUM_SNAPSHOTS_TO_KEEP, oldestFiles.size()).forEach(this::deleteSnapshotFile);
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

    private void deleteFile(File file) {
        if (file != null) {
            boolean success = file.delete();
            if (!success) {
                LOG.error("failed to delete snapshot {}", file.getName());
            }
        }
    }

    private static File createSnapshotFile(String channelName) throws IOException {
        return File.createTempFile(String.format("%s%s-", getSnapshotFileNamePrefix(channelName), dateTimeFormatter.format(Instant.now())), COMPACTION_FILE_EXTENSION);
    }

    private void uploadSnapshot(String bucketName, final File snapshotFile) {
        s3Helper.upload(bucketName, snapshotFile);
    }

    private void writeSequenceNumbers(ChannelPosition currentChannelPosition, JsonGenerator jGenerator) throws IOException {
        jGenerator.writeArrayFieldStart(START_SEQUENCE_NUMBERS_FIELD_NAME);
        currentChannelPosition.shards().forEach(shardName -> {
            try {
                jGenerator.writeStartObject();
                jGenerator.writeStringField(SHARD_FIELD_NAME, shardName);
                if (currentChannelPosition.shard(shardName).startFrom() == StartFrom.HORIZON) {
                    jGenerator.writeStringField(SEQUENCE_NUMBER_FIELD_NAME, "0");
                } else {
                    jGenerator.writeStringField(SEQUENCE_NUMBER_FIELD_NAME, currentChannelPosition.shard(shardName).position());
                }
                jGenerator.writeEndObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        jGenerator.writeEndArray();
    }


}
