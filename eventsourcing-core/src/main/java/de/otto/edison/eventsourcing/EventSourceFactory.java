package de.otto.edison.eventsourcing;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.kinesis.KinesisStream;
import de.otto.edison.eventsourcing.s3.SnapshotConsumerService;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotReadService;
import org.springframework.context.ApplicationEventPublisher;
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

    private final ApplicationEventPublisher applicationEventPublisher;

    public EventSourceFactory(
            SnapshotReadService snapshotReadService,
            SnapshotConsumerService snapshotConsumerService,
            ObjectMapper objectMapper,
            KinesisClient kinesisClient,
            TextEncryptor textEncryptor,
            ApplicationEventPublisher applicationEventPublisher) {
        this.snapshotReadService = snapshotReadService;
        this.snapshotConsumerService = snapshotConsumerService;
        this.objectMapper = objectMapper;
        this.kinesisClient = kinesisClient;
        this.textEncryptor = textEncryptor;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    public EventSource createEventSource(final Class<? extends EventSource> eventSourceClazz,
                                         final String streamName) {
        Objects.requireNonNull(eventSourceClazz, "event source class must not be null");
        Objects.requireNonNull(streamName, "stream name must not be null");

        if (eventSourceClazz.equals(SnapshotEventSource.class)) {
            return createSnapshotEventSource(streamName);
        } else if (eventSourceClazz.equals(KinesisEventSource.class)) {
            return createKinesisEventSource(streamName);
        } else if (eventSourceClazz.equals(CompactingKinesisEventSource.class)) {
            return createCompactingKinesisEventSource(streamName);
        }
        throw new IllegalArgumentException("Unknown event source type to create instance.");
    }

    public KinesisEventSource createKinesisEventSource(final String streamName) {
        KinesisStream kinesisStream = new KinesisStream(kinesisClient, streamName);
        return new KinesisEventSource(kinesisStream, textEncryptor, objectMapper);
    }

    public SnapshotEventSource createSnapshotEventSource(final String streamName) {
        return new SnapshotEventSource(streamName,
                snapshotReadService,
                snapshotConsumerService,
                applicationEventPublisher,
                objectMapper
        );
    }

    public CompactingKinesisEventSource createCompactingKinesisEventSource(final String streamName) {
        return new CompactingKinesisEventSource(
                createSnapshotEventSource(streamName),
                createKinesisEventSource(streamName)
        );
    }
}
