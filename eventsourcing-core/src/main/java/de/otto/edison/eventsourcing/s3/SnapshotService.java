package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.s3.S3Service;
import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.slf4j.Logger;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static de.otto.edison.eventsourcing.consumer.Event.event;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;
import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotService {

    private static final Logger LOG = getLogger(SnapshotService.class);

    private static final String COMPACTION_FILE_EXTENSION = ".json.zip";
    private static final DateTimeFormatter dateTimeFormatter = ofPattern("yyyy-MM-dd'T'HH-mmX").withZone(ZoneOffset.UTC);

    private JsonFactory jsonFactory = new JsonFactory();
    private S3Service s3Service;
    private String snapshotBucketTemplate;

    private final ObjectMapper objectMapper;

    private static final String ZIP_ENTRY = "data";

    //JSON Fields
    private static final String DATA_FIELD_NAME = "data";
    private static final String START_SEQUENCE_NUMBERS_FIELD_NAME = "startSequenceNumbers";
    private static final String SHARD_FIELD_NAME = "shard";
    private static final String SEQUENCE_NUMBER_FIELD_NAME = "sequenceNumber";

    public SnapshotService(final S3Service s3Service,
                           final EventSourcingProperties properties,
                           final ObjectMapper objectMapper) {
        this.s3Service = s3Service;
        snapshotBucketTemplate = properties.getSnapshot().getBucketTemplate();
        this.objectMapper = objectMapper;
    }

    Optional<File> getLatestSnapshotFromBucket(final String streamName) {
        String snapshotBucket = createBucketName(streamName, snapshotBucketTemplate);
        Optional<S3Object> s3Object = getLatestZip(snapshotBucket, streamName);
        if (s3Object.isPresent()) {
            String latestSnapshotKey = s3Object.get().key();
            Path snapshotFile = Paths.get(System.getProperty("java.io.tmpdir") + "/" + latestSnapshotKey);
            LOG.info("Downloading snapshot file to " + snapshotFile.getFileName().toAbsolutePath().toString());
            if (s3Service.download(snapshotBucket, latestSnapshotKey, snapshotFile)) {
                return Optional.of(snapshotFile.toFile());
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }

    private String createBucketName(String streamName, String snapshotBucketTemplate) {
        return snapshotBucketTemplate.replace("{stream-name}", streamName);
    }

    Optional<S3Object> getLatestZip(String bucketName, String streamName) {
        return s3Service.listAll(bucketName)
                .stream()
                .filter(o -> o.key().startsWith(getSnapshotFileNamePrefix(streamName)))
                .filter(o -> o.key().endsWith(COMPACTION_FILE_EXTENSION))
                .sorted(comparing(S3Object::lastModified, reverseOrder()))
                .findFirst();
    }


    public <T> StreamPosition consumeSnapshot(final Optional<File> latestSnapshot,
                                          final String streamName,
                                          final Predicate<Event<T>> stopCondition,
                                          final Consumer<Event<T>> consumer,
                                          final Class<T> payloadType) throws IOException {
        if (latestSnapshot.isPresent()) {
            return processSnapshotFile(latestSnapshot.get(), streamName, stopCondition, consumer, payloadType);
        } else {
            return StreamPosition.of();
        }
    }

    boolean deleteDownloadedSnapshot(File snapshotFile) {
        return snapshotFile.delete();
    }


    <T> StreamPosition processSnapshotFile(final File snapshotFile,
                                       final String streamName,
                                       final Predicate<Event<T>> stopCondition,
                                       final Consumer<Event<T>> callback,
                                       final Class<T> payloadType) throws IOException {
        try (
                FileInputStream fileInputStream = new FileInputStream(snapshotFile);
                BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                ZipInputStream zipInputStream = new ZipInputStream(bufferedInputStream)
        ) {
            StreamPosition shardPositions = StreamPosition.of();
            zipInputStream.getNextEntry();
            JsonParser parser = jsonFactory.createParser(zipInputStream);
            while (!parser.isClosed()) {
                JsonToken currentToken = parser.nextToken();
                if (currentToken == JsonToken.FIELD_NAME) {
                    switch (parser.getValueAsString()) {
                        case "startSequenceNumbers":
                            shardPositions = processSequenceNumbers(parser);
                            break;
                        case "data":
                            processSnapshotData(
                                    parser,
                                    shardPositions.positionOf(streamName),
                                    stopCondition,
                                    callback,
                                    payloadType);
                            break;
                        default:
                            break;
                    }
                }
            }
            return shardPositions;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String takeSnapshot(final String streamName,
                             final StreamPosition position,
                             final StateRepository<String> stateRepository) throws IOException {
        final File snapshotFile = createSnapshot(streamName, position, stateRepository);
        final String fileName = snapshotFile.getName();
        LOG.info(format("Finished creating snapshot file: %s", snapshotFile.getAbsolutePath()));
        uploadSnapshot(createBucketName(streamName, this.snapshotBucketTemplate), snapshotFile);
        LOG.info("Finished uploaded snapshot file to s3");
        snapshotFile.delete();
        return fileName;
    }

    private static File createSnapshotFile(String streamName) throws IOException {
        return File.createTempFile(String.format("%s%s-", getSnapshotFileNamePrefix(streamName), dateTimeFormatter.format(Instant.now())), COMPACTION_FILE_EXTENSION);
    }

    private static String getSnapshotFileNamePrefix(String streamName) {
        return String.format("compaction-%s-snapshot-", streamName);
    }

    private File createSnapshot(final String streamName,
                                final StreamPosition currentStreamPosition,
                                final StateRepository<String> stateRepository) throws IOException {
        final JsonFactory jsonFactory = new JsonFactory();
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

    private <T> void processSnapshotData(final JsonParser parser,
                                     final String sequenceNumber,
                                     final Predicate<Event<T>> stopCondition,
                                     final Consumer<Event<T>> callback,
                                     final Class<T> payloadType) throws IOException {
        // Would be better to store event meta data together with key+value:
        final Instant arrivalTimestamp = Instant.EPOCH;
        boolean abort = false;
        while (!abort && parser.nextToken() != JsonToken.END_ARRAY) {
            JsonToken currentToken = parser.currentToken();
            if (currentToken == JsonToken.FIELD_NAME) {
                final Event<T> event = event(
                        parser.getValueAsString(),
                        objectMapper.convertValue(parser.nextTextValue(), payloadType),
                        sequenceNumber,
                        arrivalTimestamp);
                callback.accept(event);
                abort = stopCondition.test(event);
            }
        }
    }

    private StreamPosition processSequenceNumbers(final JsonParser parser) throws IOException {
        final Map<String,String> shardPositions = new HashMap<>();

        String shardId = null;
        String sequenceNumber = null;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            JsonToken currentToken = parser.currentToken();
            switch (currentToken) {
                case FIELD_NAME:
                    switch (parser.getValueAsString()) {
                        case "shard":
                            parser.nextToken();
                            shardId = parser.getValueAsString();
                            break;
                        case "sequenceNumber":
                            parser.nextToken();
                            sequenceNumber = parser.getValueAsString();
                            break;
                        default:
                            break;
                    }
                    break;
                case END_OBJECT:
                    shardPositions.put(shardId, sequenceNumber);
                    shardId = null;
                    sequenceNumber = null;
                    break;
                default:
                    break;
            }
        }
        return StreamPosition.of(shardPositions);
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
