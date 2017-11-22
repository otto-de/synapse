package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.zip.ZipInputStream;

import static de.otto.edison.eventsourcing.consumer.Event.event;

@Service
public class SnapshotConsumerService {

    private final ObjectMapper objectMapper;
    private final TextEncryptor textEncryptor;
    private final JsonFactory jsonFactory = new JsonFactory();


    @Autowired
    public SnapshotConsumerService(ObjectMapper objectMapper, TextEncryptor textEncryptor) {
        this.objectMapper = objectMapper;
        this.textEncryptor = textEncryptor;
    }

    public <T> StreamPosition consumeSnapshot(final File latestSnapshot,
                                              final String streamName,
                                              final Predicate<Event<T>> stopCondition,
                                              final Consumer<Event<T>> consumer,
                                              final Class<T> payloadType) throws IOException {

        try (
                FileInputStream fileInputStream = new FileInputStream(latestSnapshot);
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
                                    consumer,
                                    payloadType);
                            break;
                        default:
                            break;
                    }
                }
            }
            return shardPositions;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
                        objectMapper.convertValue(textEncryptor.decrypt(parser.nextTextValue()), payloadType),
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
