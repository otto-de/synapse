package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.s3.S3Service;
import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.slf4j.Logger;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.zip.ZipInputStream;

import static de.otto.edison.eventsourcing.consumer.Event.event;
import static de.otto.edison.eventsourcing.s3.SnapshotUtils.COMPACTION_FILE_EXTENSION;
import static de.otto.edison.eventsourcing.s3.SnapshotUtils.createBucketName;
import static de.otto.edison.eventsourcing.s3.SnapshotUtils.getSnapshotFileNamePrefix;
import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;
import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotReadService {

    private static final Logger LOG = getLogger(SnapshotReadService.class);

    private JsonFactory jsonFactory = new JsonFactory();
    private S3Service s3Service;
    private String snapshotBucketTemplate;

    private final ObjectMapper objectMapper;

    public SnapshotReadService(final S3Service s3Service,
                               final EventSourcingProperties properties,
                               final ObjectMapper objectMapper) {
        this.s3Service = s3Service;
        snapshotBucketTemplate = properties.getSnapshot().getBucketTemplate();
        this.objectMapper = objectMapper;
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

    public Optional<File> getLatestSnapshotFromBucket(final String streamName) {
        String snapshotBucket = createBucketName(streamName, snapshotBucketTemplate);
        Optional<S3Object> s3Object = getLatestZip(snapshotBucket, streamName);
        if (s3Object.isPresent()) {
            String latestSnapshotKey = s3Object.get().key();
            Path snapshotFile = Paths.get(System.getProperty("java.io.tmpdir") + "/" + latestSnapshotKey);
            LOG.info("Downloading snapshot file to {}", snapshotFile.getFileName().toAbsolutePath().toString());
            if (s3Service.download(snapshotBucket, latestSnapshotKey, snapshotFile)) {
                return Optional.of(snapshotFile.toFile());
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }

    public boolean deleteDownloadedSnapshot(File snapshotFile) {
        return snapshotFile.delete();
    }

    Optional<S3Object> getLatestZip(String bucketName, String streamName) {
        return s3Service.listAll(bucketName)
                .stream()
                .filter(o -> o.key().startsWith(getSnapshotFileNamePrefix(streamName)))
                .filter(o -> o.key().endsWith(COMPACTION_FILE_EXTENSION))
                .sorted(comparing(S3Object::lastModified, reverseOrder()))
                .findFirst();
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
        final Map<String, String> shardPositions = new HashMap<>();

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

}
