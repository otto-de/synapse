package de.otto.synapse.aws.s3;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import de.otto.edison.aws.s3.S3Service;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.configuration.aws.SnapshotProperties;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;

import java.io.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static de.otto.synapse.aws.s3.SnapshotUtils.COMPACTION_FILE_EXTENSION;
import static de.otto.synapse.aws.s3.SnapshotUtils.getSnapshotFileNamePrefix;
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

    private final S3Service s3Service;
    private final String snapshotBucketName;

    private final JsonFactory jsonFactory = new JsonFactory();

    public SnapshotWriteService(final S3Service s3Service,
                                final SnapshotProperties properties) {
        this.s3Service = s3Service;
        this.snapshotBucketName = properties.getBucketName();
    }


    public String writeSnapshot(final String streamName,
                                final ChannelPosition position,
                                final StateRepository<String> stateRepository) throws IOException {
        File snapshotFile = null;
        try {
            LOG.info("Start creating new snapshot");
            snapshotFile = createSnapshot(streamName, position, stateRepository);
            LOG.info("Finished creating snapshot file: {}", snapshotFile.getAbsolutePath());
            uploadSnapshot(this.snapshotBucketName, snapshotFile);
            LOG.info("Finished uploading snapshot file to s3");
        } finally {
            if (snapshotFile != null) {
                LOG.info("delete file {}", snapshotFile.toPath().toString());
                deleteFile(snapshotFile);
            }
        }
        return snapshotFile.getName();
    }

    @VisibleForTesting
    File createSnapshot(final String streamName,
                        final ChannelPosition currentChannelPosition,
                        final StateRepository<String> stateRepository) throws IOException {
        File snapshotFile = createSnapshotFile(streamName);

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
            stateRepository.getKeySetIterable().forEach((key) -> {
                try {
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

    private void deleteFile(File file) {
        if (file != null) {
            boolean success = file.delete();
            if (!success) {
                LOG.error("failed to delete snapshot {}", file.getName());
            }
        }
    }

    private static File createSnapshotFile(String streamName) throws IOException {
        return File.createTempFile(String.format("%s%s-", getSnapshotFileNamePrefix(streamName), dateTimeFormatter.format(Instant.now())), COMPACTION_FILE_EXTENSION);
    }

    private void uploadSnapshot(String bucketName, final File snapshotFile) {
        s3Service.upload(bucketName, snapshotFile);
    }

    private void writeSequenceNumbers(ChannelPosition currentChannelPosition, JsonGenerator jGenerator) throws IOException {
        jGenerator.writeArrayFieldStart(START_SEQUENCE_NUMBERS_FIELD_NAME);
        currentChannelPosition.shards().forEach(shardId -> {
            try {
                jGenerator.writeStartObject();
                jGenerator.writeStringField(SHARD_FIELD_NAME, shardId);
                jGenerator.writeStringField(SEQUENCE_NUMBER_FIELD_NAME, currentChannelPosition.positionOf(shardId));
                jGenerator.writeEndObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        jGenerator.writeEndArray();
    }


}
