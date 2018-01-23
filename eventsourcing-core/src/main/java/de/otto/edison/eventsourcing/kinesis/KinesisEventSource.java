package de.otto.edison.eventsourcing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.TemporaryDecryption;
import de.otto.edison.eventsourcing.consumer.AbstractEventSource;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static de.otto.edison.eventsourcing.kinesis.KinesisEvent.kinesisEvent;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;

public class KinesisEventSource extends AbstractEventSource {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisEventSource.class);

    private final KinesisStream kinesisStream;
    private final Function<String, String> deserializer;
    private final TextEncryptor textEncryptor;

    public KinesisEventSource(final String name,
                              final KinesisStream kinesisStream,
                              final TextEncryptor textEncryptor,
                              final ObjectMapper objectMapper) {
        super(name, objectMapper);
        this.deserializer = textEncryptor::decrypt;
        this.textEncryptor = textEncryptor;
        this.kinesisStream = kinesisStream;
    }

    @Override
    public String getStreamName() {
        return kinesisStream.getStreamName();
    }

    @Override
    public StreamPosition consumeAll(final StreamPosition startFrom,
                                     final Predicate<Event<?>> stopCondition) {

        List<KinesisShard> kinesisShards = kinesisStream.retrieveAllOpenShards();
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(kinesisShards.size(), 10));

        List<CompletableFuture<ShardPosition>> futureShardPositions = kinesisShards
                .stream()
                .map(shard -> CompletableFuture.supplyAsync(
                        () -> shard.consumeRecordsAndReturnLastSeqNumber(
                                startFrom.positionOf(shard.getShardId()),
                                recordStopCondition(stopCondition),
                                recordConsumer()), executorService))
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

    private Event<String> createEvent(Duration durationBehind, Record record) {
        return kinesisEvent(durationBehind, record, byteBuffer -> {
            String json = UTF_8.decode(record.data()).toString();
            return TemporaryDecryption.decryptIfNecessary(json, textEncryptor);
        });
    }

    private BiFunction<Long, Record, Boolean> recordStopCondition(final Predicate<Event<?>> stopCondition) {
        return (millis, record) -> {
            if (record == null) {
                return stopCondition.test(Event.event(null, null, null, null, ofMillis(millis)));
            }
            return stopCondition.test(createEvent(ofMillis(millis), record));
        };
    }

    private BiConsumer<Long, Record> recordConsumer() {
        return (millis, record) -> {
            final Event<String> jsonEvent = createEvent(ofMillis(millis), record);
            registeredConsumers().encodeAndSend(jsonEvent);
        };
    }
}
