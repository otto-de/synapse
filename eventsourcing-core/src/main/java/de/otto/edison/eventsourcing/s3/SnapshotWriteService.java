package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import de.otto.edison.aws.s3.S3Service;
import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.io.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static de.otto.edison.eventsourcing.s3.SnapshotUtils.COMPACTION_FILE_EXTENSION;
import static de.otto.edison.eventsourcing.s3.SnapshotUtils.getSnapshotFileNamePrefix;
import static java.time.format.DateTimeFormatter.ofPattern;
import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotWriteService {
    private static final Logger LOG = getLogger(SnapshotReadService.class);

    private static final DateTimeFormatter dateTimeFormatter = ofPattern("yyyy-MM-dd'T'HH-mmX").withZone(ZoneOffset.UTC);

    private static final String ZIP_ENTRY = "data";
    //JSON Fields
    private static final String DATA_FIELD_NAME = "data";
    private static final String START_SEQUENCE_NUMBERS_FIELD_NAME = "startSequenceNumbers";
    private static final String SHARD_FIELD_NAME = "shard";
    private static final String SEQUENCE_NUMBER_FIELD_NAME = "sequenceNumber";

    private final S3Service s3Service;
    private final TextEncryptor textEncryptor;

    private final JsonFactory jsonFactory = new JsonFactory();

    private final S3Client s3Client;
    private final EventSourcingProperties eventSourcingProperties;

    @Autowired
    public SnapshotWriteService(final S3Service s3Service,
                                final EventSourcingProperties properties,
                                final TextEncryptor textEncryptor,
                                final S3Client s3Client) {
        this.s3Service = s3Service;
        this.eventSourcingProperties = properties;
        this.textEncryptor = textEncryptor;
        this.s3Client = s3Client;
    }


    public String takeSnapshot(final String streamName,
                               final StreamPosition position,
                               final StateRepository<String> stateRepository) throws IOException {
        File snapshotFile = null;
        try {
            LOG.info("Start creating new snapshot");
            snapshotFile = createSnapshot(streamName, position, stateRepository);
            LOG.info("Finished creating snapshot file: {}", snapshotFile.getAbsolutePath());
            uploadSnapshot(eventSourcingProperties.getSnapshot().getBucketName(), snapshotFile);
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
                        final StreamPosition currentStreamPosition,
                        final StateRepository<String> stateRepository) throws IOException {
        File snapshotFile = createSnapshotFile(streamName);
        FileOutputStream fos = new FileOutputStream(snapshotFile);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        ZipOutputStream zipOutputStream = new ZipOutputStream(bos);
        ZipEntry zipEntry = new ZipEntry(ZIP_ENTRY);
        zipEntry.setMethod(ZipEntry.DEFLATED);
        zipOutputStream.putNextEntry(zipEntry);
        JsonGenerator jGenerator = jsonFactory.createGenerator(zipOutputStream, JsonEncoding.UTF8);

        try {
            jGenerator.writeStartObject();
            writeSequenceNumbers(currentStreamPosition, jGenerator);
            // write to data file
            jGenerator.writeArrayFieldStart(DATA_FIELD_NAME);
            stateRepository.getKeySetIterable().forEach((key) -> {
                try {
                    String entry = stateRepository.get(key).get();
                    if (!("".equals(entry))) {
                        // entry = decryptIfNecessary(entry);
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
        } catch (Exception e) {
            LOG.info("delete file {}", snapshotFile.toPath().toString());
            deleteFile(snapshotFile);
            throw e;
        } finally {
            jGenerator.flush();
            zipOutputStream.closeEntry();
            zipOutputStream.close();
            jGenerator.close();
            bos.close();
            fos.close();
            System.gc();
        }
        return snapshotFile;
    }

    @VisibleForTesting
    String decryptIfNecessary(String entry) {
        if (entry.startsWith("{")) {
            return entry;
        } else {
            return textEncryptor.decrypt(entry);
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

    private static File createSnapshotFile(String streamName) throws IOException {
        return File.createTempFile(String.format("%s%s-", getSnapshotFileNamePrefix(streamName), dateTimeFormatter.format(Instant.now())), COMPACTION_FILE_EXTENSION);
    }

    private void uploadSnapshot(String bucketName, final File snapshotFile) {
        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(snapshotFile.getName());

        if ("kms".equals(eventSourcingProperties.getSnapshot().getSseType())) {
            putObjectRequestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
            putObjectRequestBuilder.ssekmsKeyId(eventSourcingProperties.getSnapshot().getSseKey());
        }

        final PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequestBuilder.build(),
                snapshotFile.toPath());

        LOG.debug("upload {} to bucket {}: ", snapshotFile.getName(), bucketName, putObjectResponse.toString());

    }

    private void writeSequenceNumbers(StreamPosition currentStreamPosition, JsonGenerator jGenerator) throws IOException {
        jGenerator.writeArrayFieldStart(START_SEQUENCE_NUMBERS_FIELD_NAME);
        currentStreamPosition.shards().forEach(shardId -> {
            try {
                jGenerator.writeStartObject();
                jGenerator.writeStringField(SHARD_FIELD_NAME, shardId);
                jGenerator.writeStringField(SEQUENCE_NUMBER_FIELD_NAME, currentStreamPosition.positionOf(shardId));
                jGenerator.writeEndObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        jGenerator.writeEndArray();
    }


}
