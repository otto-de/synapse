package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static de.otto.edison.eventsourcing.kinesis.KinesisEvent.kinesisEvent;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;
import static java.util.stream.Collectors.toMap;

public class KinesisEventSource<T> implements EventSource<T> {

    private KinesisStream kinesisStream;

    private Class<T> payloadType;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KinesisClient kinesisClient;

    // required by EnableEventSourceImportSelector
    public KinesisEventSource(final String streamName,
                              final Class<T> payloadType) {
        this.payloadType = payloadType;
        this.kinesisStream = new KinesisStream(kinesisClient, streamName);
    }

    public KinesisEventSource(final Class<T> payloadType,
                              final ObjectMapper objectMapper,
                              final KinesisStream kinesisStream) {
        this.payloadType = payloadType;
        this.objectMapper = objectMapper;
        this.kinesisStream = kinesisStream;
    }

    @Override
    public String name() {
        return kinesisStream.getStreamName();
    }

    @Override
    public StreamPosition consumeAll(final StreamPosition startFrom,
                                     final Predicate<Event<T>> stopCondition,
                                     final Consumer<Event<T>> consumer) {
        Map<String, String> result = kinesisStream.retrieveAllOpenShards()
                .stream()
                .parallel()
                .map(shard -> shard.consumeRecordsAndReturnLastSeqNumber(
                        startFrom.positionOf(shard.getShardId()),
                        new RecordStopCondition(stopCondition),
                        new RecordConsumer(consumer)))
                .collect(toMap(
                        ShardPosition::getShardId,
                        ShardPosition::getSequenceNumber));
        return StreamPosition.of(result);
    }

    private Event<T> createEvent(Duration durationBehind, Record record) {
        return kinesisEvent(durationBehind, record, byteBuffer -> {
            try {
                final String json = UTF_8.decode(record.data()).toString();
                return objectMapper.readValue(json, payloadType);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private class RecordStopCondition implements BiFunction<Long, Record, Boolean> {
        private final Predicate<Event<T>> stopCondition;

        RecordStopCondition(Predicate<Event<T>> stopCondition) {
            this.stopCondition = stopCondition;
        }

        @Override
        public Boolean apply(Long millis, Record record) {
            if (record == null) {
                return stopCondition.test(null);
            }
            return stopCondition.test(createEvent(ofMillis(millis), record));
        }
    }

    private class RecordConsumer implements BiConsumer<Long, Record> {
        private final Consumer<Event<T>> consumer;

        RecordConsumer(Consumer<Event<T>> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void accept(Long millis, Record record) {
            consumer.accept(createEvent(ofMillis(millis), record));
        }
    }
}
