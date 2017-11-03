package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.slf4j.Logger;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

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

    private KinesisClient kinesisClient;
    private String streamName;
    // TODO: inject objectmapper
    private ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> payloadType;

    public KinesisEventSource(final KinesisClient kinesisClient,
                              final String streamName,
                              final Class<T> payloadType) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.payloadType = payloadType;
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
        consumer.init(streamName);
        try {
            Map<String, String> result = KinesisUtils.retrieveAllOpenShards(kinesisClient, streamName)
                    .stream()
                    .parallel()
                    .map(shard -> {
                        final String startPosition = startFrom.positionOf(shard.shardId());
                        return consumeShard(shard.shardId(), stopCondition, startPosition, consumer);
                    })
                    .collect(toMap(
                            ShardPosition::getShardId,
                            ShardPosition::getSequenceNumber));
            consumer.completed(streamName);
            return StreamPosition.of(result);
        } catch (final RuntimeException e) {
            consumer.aborted(streamName);
            throw e;
        }
    }

    private ShardPosition consumeShard(final String shardId,
                                         final Predicate<Event<T>> stopCondition,
                                         final String shardPosition,
                                         final EventConsumer<T> consumer) {
        LOG.info(format(
                "Reading from stream %s, shard %s with starting sequence number %s",
                streamName, shardId, shardPosition));

        GetShardIteratorResponse shardIterator;
        try {
            shardIterator = kinesisClient.getShardIterator(buildIteratorShardRequest(streamName, shardId, shardPosition));
        } catch (final InvalidArgumentException e) {
            LOG.error(format("invalidShardSequenceNumber in Snapshot %s/%s - reading from HORIZON", streamName, shardId));
            shardIterator = kinesisClient.getShardIterator(buildIteratorShardRequest(streamName, shardId, "0"));
        }

        String lastSequenceNumber = retrieveDataFromSingleShard(streamName, shardIterator, stopCondition, consumer);

        String sequenceNumber = lastSequenceNumber != null
                ? lastSequenceNumber
                : Objects.toString(shardPosition, "0");

        return new ShardPosition(shardId, sequenceNumber);
    }

    private GetShardIteratorRequest buildIteratorShardRequest(String streamName, String shardId, String shardPosition) {
        GetShardIteratorRequest.Builder shardRequestBuilder = GetShardIteratorRequest
                .builder()
                .shardId(shardId)
                .streamName(streamName);

        if (shardPosition == null || shardPosition.equals("0")) {
            shardRequestBuilder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        } else {
            shardRequestBuilder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            shardRequestBuilder.startingSequenceNumber(shardPosition);
        }

        return shardRequestBuilder.build();
    }

    private String retrieveDataFromSingleShard(String streamName,
                                               GetShardIteratorResponse initialShardIterator,
                                               Predicate<Event<T>> stopCondition,
                                               EventConsumer<T> consumer) {
        String shardIterator = initialShardIterator.shardIterator();
        String lastSequenceNumber = null;
        boolean stopRetrieval;
        do {
            GetRecordsResponse recordsResponse = kinesisClient.getRecords(GetRecordsRequest.builder()
                    .shardIterator(shardIterator)
                    .build());

            stopRetrieval = isEmptyStream(recordsResponse);

            if (!stopRetrieval) {
                shardIterator = recordsResponse.nextShardIterator();
                final Duration durationBehind = ofMillis(recordsResponse.millisBehindLatest());
                for (final Record record : recordsResponse.records()) {
                    final Event<T> event = kinesisEvent(streamName, durationBehind, record, byteBuffer -> {
                        final Object json = UTF_8.decode(record.data()).toString();
                        return objectMapper.convertValue(json, payloadType);
                    });

                    stopRetrieval = stopCondition.test(event);
                    consumer.accept(event);
                    lastSequenceNumber = event.sequenceNumber();
                }
                final String durationString = format("%s days %s hrs %s min %s sec", durationBehind.toDays(), durationBehind.toHours() % 24, durationBehind.toMinutes() % 60, durationBehind.getSeconds() % 60);
                LOG.info(format(
                        "Consumed %s records from kinesis %s; behind latest: %s",
                        recordsResponse.records().size(),
                        streamName,
                        durationString));

            }
        } while (!stopRetrieval);

        return lastSequenceNumber;
    }

    private boolean isEmptyStream(GetRecordsResponse recordsResponse) {
        return recordsResponse.records().isEmpty() && recordsResponse.millisBehindLatest() <= 0.0d;
    }

}
