package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static de.otto.edison.eventsourcing.kinesis.KinesisEvent.kinesisEvent;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

public class KinesisEventSource<T> implements EventSource<T> {

    private static final Logger LOG = getLogger(KinesisEventSource.class);

    private String streamName;
    private KinesisStream kinesisStream;

    private Class<T> payloadType;

    @Autowired
    private KinesisUtils kinesisUtils;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KinesisClient kinesisClient;

    // required by EnableEventSourceImportSelector
    public KinesisEventSource(final String streamName,
                              final Class<T> payloadType) {
        this.streamName = streamName;
        this.payloadType = payloadType;
        this.kinesisStream = new KinesisStream(kinesisClient, streamName);
    }

    public KinesisEventSource(final KinesisUtils kinesisUtils,
                              final String streamName,
                              final Class<T> payloadType,
                              final ObjectMapper objectMapper,
                              final KinesisStream kinesisStream) {
        this.kinesisUtils = kinesisUtils;
        this.streamName = streamName;
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
        return streamName;
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
                        return consumeShard(shard.getShardId(), stopCondition, startPosition, consumer);
                    })
                    .collect(toMap(
                            ShardPosition::getShardId,
                            ShardPosition::getSequenceNumber));
            return StreamPosition.of(result);
        } catch (final RuntimeException e) {
            throw e;
        }
    }

    private ShardPosition consumeShard(final String shardId,
                                       final Predicate<Event<T>> stopCondition,
                                       final String shardPosition,
                                       final EventConsumer<T> consumer) {
        LOG.info("Reading from stream {}, shard {} with starting sequence number {}", streamName, shardId, shardPosition);

        String shardIterator = kinesisUtils.getShardIterator(streamName, shardId, shardPosition);
        String lastSequenceNumber = retrieveDataFromSingleShard(streamName, shardIterator, stopCondition, consumer);

        String sequenceNumber = (lastSequenceNumber != null)
                ? lastSequenceNumber
                : Objects.toString(shardPosition, "0");

        return new ShardPosition(shardId, sequenceNumber);
    }


    private String retrieveDataFromSingleShard(String streamName,
                                               String initialShardIterator,
                                               Predicate<Event<T>> stopCondition,
                                               EventConsumer<T> consumer) {
        String shardIterator = initialShardIterator;
        String lastSequenceNumber = null;
        boolean stopRetrieval;
        do {
            GetRecordsResponse recordsResponse = kinesisUtils.getRecords(shardIterator);
            shardIterator = recordsResponse.nextShardIterator();

            stopRetrieval = stopCondition.test(null);
            if (!isEmptyStream(recordsResponse)) {
                Duration durationBehind = ofMillis(recordsResponse.millisBehindLatest());
                for (final Record record : recordsResponse.records()) {
                    Event<T> event = createEvent(durationBehind, record);
                    consumer.accept(event);
                    stopRetrieval = stopCondition.test(event);
                    lastSequenceNumber = event.sequenceNumber();
                }

                logInfo(streamName, recordsResponse, durationBehind);
            }
            if (!stopRetrieval) {
                stopRetrieval = waitABit();
            }
        } while (!stopRetrieval);
        LOG.info("Terminating event source for stream {}", streamName);
        return lastSequenceNumber;
    }

    private void logInfo(String streamName, GetRecordsResponse recordsResponse, Duration durationBehind) {
        final String durationString = format("%s days %s hrs %s min %s sec", durationBehind.toDays(), durationBehind.toHours() % 24, durationBehind.toMinutes() % 60, durationBehind.getSeconds() % 60);
        LOG.info("Consumed {} records from kinesis {}; behind latest: {}",
                recordsResponse.records().size(),
                streamName,
                durationString);
    }

    @NotNull
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

    private boolean waitABit() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOG.warn("Thread got interrupted");
            return true;
        }
        return false;
    }

    private boolean isEmptyStream(GetRecordsResponse recordsResponse) {
        return recordsResponse.records().isEmpty();
    }

}
