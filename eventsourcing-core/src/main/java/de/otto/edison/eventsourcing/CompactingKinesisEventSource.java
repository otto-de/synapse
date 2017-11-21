package de.otto.edison.eventsourcing;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.kinesis.KinesisStream;
import de.otto.edison.eventsourcing.s3.SnapshotConsumerService;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;
import de.otto.edison.eventsourcing.s3.SnapshotReadService;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class CompactingKinesisEventSource<T> implements EventSource<T> {

    private SnapshotReadService snapshotService;
    private SnapshotConsumerService snapshotConsumerService;
    private Function<String, T> deserializer;
    private KinesisClient kinesisClient;

    private final String streamName;
    private final Class<T> payloadType;

    public CompactingKinesisEventSource(String streamName,
                                        Class<T> payloadType,
                                        SnapshotReadService snapshotService,
                                        SnapshotConsumerService snapshotConsumerService,
                                        ObjectMapper objectMapper,
                                        KinesisClient kinesisClient) {
        this.streamName = streamName;
        this.payloadType = payloadType;
        this.snapshotService = snapshotService;
        this.snapshotConsumerService = snapshotConsumerService;
        this.deserializer = in -> {
            try {
                return objectMapper.readValue(in, payloadType);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        this.kinesisClient = kinesisClient;
    }

    public CompactingKinesisEventSource(String streamName,
                                        Class<T> payloadType,
                                        SnapshotReadService snapshotService,
                                        SnapshotConsumerService snapshotConsumerService,
                                        Function<String, T> deserializer,
                                        KinesisClient kinesisClient) {
        this.streamName = streamName;
        this.payloadType = payloadType;
        this.snapshotService = snapshotService;
        this.snapshotConsumerService = snapshotConsumerService;
        this.deserializer = deserializer;
        this.kinesisClient = kinesisClient;
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<T>> stopCondition, Consumer<Event<T>> consumer) {
        final SnapshotEventSource<T> snapshotEventSource = new SnapshotEventSource<>(streamName, snapshotService, snapshotConsumerService, payloadType);

        KinesisStream kinesisStream = new KinesisStream(kinesisClient, streamName);
        final KinesisEventSource<T> kinesisEventSource = new KinesisEventSource<>(deserializer, kinesisStream);

        final StreamPosition streamPosition = snapshotEventSource.consumeAll(stopCondition, consumer);
        return kinesisEventSource.consumeAll(streamPosition, stopCondition, consumer);
    }
}
