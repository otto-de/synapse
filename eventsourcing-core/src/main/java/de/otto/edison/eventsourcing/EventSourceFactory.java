package de.otto.edison.eventsourcing;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.kinesis.KinesisStream;
import de.otto.edison.eventsourcing.s3.SnapshotConsumerService;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotReadService;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.util.Objects;

@Component
public class EventSourceFactory {

    private final SnapshotReadService snapshotReadService;
    private final SnapshotConsumerService snapshotConsumerService;

    private final ObjectMapper objectMapper;
    private final KinesisClient kinesisClient;
    private final TextEncryptor textEncryptor;

    public EventSourceFactory(
            SnapshotReadService snapshotReadService,
            SnapshotConsumerService snapshotConsumerService,
            ObjectMapper objectMapper,
            KinesisClient kinesisClient,
            TextEncryptor textEncryptor) {
        this.snapshotReadService = snapshotReadService;
        this.snapshotConsumerService = snapshotConsumerService;
        this.objectMapper = objectMapper;
        this.kinesisClient = kinesisClient;
        this.textEncryptor = textEncryptor;
    }

    public <T> EventSource<T> createEventSource(Class<? extends EventSource> eventSourceClazz, String streamName, Class<T> payloadClazz) {
        Objects.requireNonNull(eventSourceClazz, "event source class must not be null");
        Objects.requireNonNull(streamName, "stream name must not be null");
        Objects.requireNonNull(payloadClazz, "payload class must not be null");

        if (eventSourceClazz.equals(SnapshotEventSource.class)) {
            return createSnapshotEventSource(streamName, payloadClazz);
        } else if (eventSourceClazz.equals(KinesisEventSource.class)) {
            return createKinesisEventSource(streamName, payloadClazz);
        } else if (eventSourceClazz.equals(CompactingKinesisEventSource.class)) {
            return createCompactingKinesisEventSource(streamName, payloadClazz);
        }
        throw new IllegalArgumentException("Unknown event source type to create instance.");
    }

    public <T> KinesisEventSource<T> createKinesisEventSource(String streamName, Class<T> payloadClazz) {
        KinesisStream kinesisStream = new KinesisStream(kinesisClient, streamName, objectMapper, textEncryptor);
        return new KinesisEventSource<>(payloadClazz, objectMapper, kinesisStream, textEncryptor);
    }

    public <T> SnapshotEventSource<T> createSnapshotEventSource(String streamName, Class<T> payloadClazz) {
        return new SnapshotEventSource<>(streamName, snapshotReadService, snapshotConsumerService, payloadClazz);
    }

    public <T> CompactingKinesisEventSource<T> createCompactingKinesisEventSource(String streamName, Class<T> payloadClazz) {
        return new CompactingKinesisEventSource<>(
                createSnapshotEventSource(streamName, payloadClazz),
                createKinesisEventSource(streamName, payloadClazz));
    }
}
