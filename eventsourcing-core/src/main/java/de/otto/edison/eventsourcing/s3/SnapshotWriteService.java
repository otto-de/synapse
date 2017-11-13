package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import de.otto.edison.aws.s3.S3Service;
import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.slf4j.Logger;

import java.io.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static de.otto.edison.eventsourcing.s3.SnapshotUtils.*;
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
    private final String snapshotBucketTemplate;

    private JsonFactory jsonFactory = new JsonFactory();

    public SnapshotWriteService(final S3Service s3Service,
                                final EventSourcingProperties properties) {
        this.s3Service = s3Service;
        snapshotBucketTemplate = properties.getSnapshot().getBucketTemplate();
    }


    public String takeSnapshot(final String streamName,
                               final StreamPosition position,
                               final StateRepository<String> stateRepository) throws IOException {
        final File snapshotFile = createSnapshot(streamName, position, stateRepository);
        final String fileName = snapshotFile.getName();
        LOG.info("Finished creating snapshot file: {}", snapshotFile.getAbsolutePath());
        uploadSnapshot(createBucketName(streamName, this.snapshotBucketTemplate), snapshotFile);
        LOG.info("Finished uploaded snapshot file to s3");
        snapshotFile.delete();
        return fileName;
    }

    private File createSnapshot(final String streamName,
                                final StreamPosition currentStreamPosition,
                                final StateRepository<String> stateRepository) throws IOException {
        File dataFile = createSnapshotFile(streamName);

        try (FileOutputStream fos = new FileOutputStream(dataFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             ZipOutputStream zipOutputStream = new ZipOutputStream(bos)
        ) {
            ZipEntry zipEntry = new ZipEntry(ZIP_ENTRY);
            zipEntry.setMethod(ZipEntry.DEFLATED);
            zipOutputStream.putNextEntry(zipEntry);
            JsonGenerator jGenerator = jsonFactory.createGenerator(zipOutputStream, JsonEncoding.UTF8);
            jGenerator.writeStartObject();
            writeSequenceNumbers(currentStreamPosition, jGenerator);
            // write to data file
            jGenerator.writeArrayFieldStart(DATA_FIELD_NAME);
            stateRepository.getKeySetIterable().forEach((key) -> {
                try {
                    jGenerator.writeStartObject();
                    jGenerator.writeStringField(key, stateRepository.get(key).get());
                    jGenerator.writeEndObject();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            jGenerator.writeEndArray();
            jGenerator.writeEndObject();
            jGenerator.flush();
            zipOutputStream.closeEntry();
        }
        return dataFile;
    }

    private static File createSnapshotFile(String streamName) throws IOException {
        return File.createTempFile(String.format("%s%s-", getSnapshotFileNamePrefix(streamName), dateTimeFormatter.format(Instant.now())), COMPACTION_FILE_EXTENSION);
    }

    private void uploadSnapshot(String bucketName, final File snapshotFile) {
        s3Service.upload(bucketName, snapshotFile);
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
