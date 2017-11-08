package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static de.otto.edison.eventsourcing.kinesis.KinesisEvent.kinesisEvent;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

public class KinesisEventSource<T> implements EventSource<T> {

    private static final Logger LOG = getLogger(KinesisEventSource.class);

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

    /**
     * Returns the name of the EventSource.
     * <p>
     * For streaming event-sources, this is the name of the event stream.
     * </p>
     *
     * @return name
     */
    @Override
    public String name() {
        return kinesisStream.getStreamName();
    }

    /**
     * Consumes all events from the EventSource, beginning with {@link StreamPosition startFrom}, until
     * the {@link Predicate stopCondition} is met.
     * <p>
     * The {@link EventConsumer consumer} will be called zero or more times, depending on
     * the number of events retrieved from the EventSource.
     * </p>
     *
     * @param startFrom     the read position returned from earlier executions
     * @param stopCondition the predicate used as a stop condition
     * @param consumer      consumer used to process events
     * @return the new read position
     */
    @Override
    public StreamPosition consumeAll(final StreamPosition startFrom,
                                     final Predicate<Event<T>> stopCondition,
                                     final EventConsumer<T> consumer) {
        try {
            Map<String, String> result = kinesisStream.retrieveAllOpenShards()
                    .stream()
                    .parallel()
                    .map(shard -> {
                        final String startPosition = startFrom.positionOf(shard.getShardId());
                        return consumeShard(shard, stopCondition, startPosition, consumer);
                    })
                    .collect(toMap(
                            ShardPosition::getShardId,
                            ShardPosition::getSequenceNumber));
            return StreamPosition.of(result);
        } catch (final RuntimeException e) {
            throw e;
        }
    }

    private ShardPosition consumeShard(final KinesisShard shard,
                                       final Predicate<Event<T>> stopCondition,
                                       final String sequenceNumber,
                                       final EventConsumer<T> consumer) {
        LOG.info("Reading from stream {}, shard {} with starting sequence number {}", name(), shard, sequenceNumber);

        String lastSequenceNumber = shard.consumeRecordsAndReturnLastSeqNumber(
                sequenceNumber,
                new RecordStopCondition(stopCondition),
                new RecordConsumer(consumer));

        String nextSequenceNumber = (lastSequenceNumber != null)
                ? lastSequenceNumber
                : Objects.toString(sequenceNumber, "0");

        return new ShardPosition(shard.getShardId(), nextSequenceNumber);
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
        private final EventConsumer<T> consumer;

        RecordConsumer(EventConsumer<T> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void accept(Long millis, Record record) {
            consumer.accept(createEvent(ofMillis(millis), record));
        }
    }
}
