package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.*;
import java.util.stream.Collectors;

import static de.otto.edison.eventsourcing.kinesis.KinesisEvent.kinesisEvent;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;

public class KinesisEventSource<T> implements EventSource<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisEventSource.class);

    private KinesisStream kinesisStream;
    private Function<String, T> deserializer;

    public KinesisEventSource(final Class<T> payloadType,
                              final ObjectMapper objectMapper,
                              final KinesisStream kinesisStream,
                              final TextEncryptor textEncryptor) {
        this.deserializer = in -> {
            try {
                if (payloadType == String.class) {
                    return (T) textEncryptor.decrypt(in);
                } else {
                    return objectMapper.readValue(textEncryptor.decrypt(in), payloadType);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        this.kinesisStream = kinesisStream;
    }

    @Override
    public String getStreamName() {
        return kinesisStream.getStreamName();
    }

    @Override
    public StreamPosition consumeAll(final StreamPosition startFrom,
                                     final Predicate<Event<T>> stopCondition,
                                     final EventConsumer<T> consumer) {

        List<KinesisShard> kinesisShards = kinesisStream.retrieveAllOpenShards();
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(kinesisShards.size(), 10));

        List<CompletableFuture<ShardPosition>> futureShardPositions = kinesisShards
                .stream()
                .map(shard -> CompletableFuture.supplyAsync(
                        () -> shard.consumeRecordsAndReturnLastSeqNumber(
                                startFrom.positionOf(shard.getShardId()),
                                new RecordStopCondition(stopCondition),
                                new RecordConsumer(consumer)), executorService))
                .collect(Collectors.toList());
        // don't chain futureShardPositions with CompletableFuture::join as lazy execution will prevent threads from
        // running in parallel

        try {
            Map<String, String> shardPositions = futureShardPositions.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toMap(ShardPosition::getShardId, ShardPosition::getSequenceNumber));

            return StreamPosition.of(shardPositions);
        } finally {
            stopAllThreads(executorService);
        }
    }

    private void stopAllThreads(ExecutorService executorService) {
        //When an exception occurs in a completable future's thread, other threads continue running.
        //Stop all before proceeding.
        executorService.shutdownNow();
        try {
            boolean allThreadsSafelyTerminated = executorService.awaitTermination(30, TimeUnit.SECONDS);
            if (!allThreadsSafelyTerminated) {
                LOG.error("Kinesis Threads still running");
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Event<T> createEvent(Duration durationBehind, Record record) {
        return kinesisEvent(durationBehind, record, byteBuffer -> {
            final String json = UTF_8.decode(record.data()).toString();
            return deserializer.apply(json);
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
                return stopCondition.test(Event.event(null, null, null, null, ofMillis(millis)));
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
