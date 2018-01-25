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
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static de.otto.edison.eventsourcing.kinesis.KinesisEvent.kinesisEvent;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;

public class NoSnapshotEventSourceKinesisEventSource extends AbstractEventSource {

    private static final Logger LOG = LoggerFactory.getLogger(NoSnapshotEventSourceKinesisEventSource.class);
    private KinesisStream kinesisStream;
    private final TextEncryptor textEncryptor;
    private final Instant startFrom;

    public NoSnapshotEventSourceKinesisEventSource(final String name,
                                                   final KinesisStream kinesisStream,
                                                   final TextEncryptor textEncryptor,
                                                   final ObjectMapper objectMapper,
                                                   final Instant startFrom) {
        super(name, objectMapper);
        this.kinesisStream = kinesisStream;
        this.textEncryptor = textEncryptor;
        this.startFrom = startFrom;
    }

    @Override
    public String getStreamName() {
        return kinesisStream.getStreamName();
    }

    @Override
    public StreamPosition consumeAll() {
        StreamPosition ignored = StreamPosition.of();
        return consumeAll(ignored, (ignore) -> true);
    }

    @Override
    public StreamPosition consumeAll(StreamPosition startFrom) {
        return null;
    }

    @Override
    public StreamPosition consumeAll(Predicate<Event<?>> stopCondition) {
        StreamPosition ignored = StreamPosition.of();
        return consumeAll(ignored, stopCondition);
    }

    @Override
    public StreamPosition consumeAll(final StreamPosition startFrom,
                                     final Predicate<Event<?>> stopCondition) {

        List<KinesisShard> kinesisShards = kinesisStream.retrieveAllOpenShards();
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(kinesisShards.size(), 10));

        kinesisShards
                .stream()
                .map(shard -> CompletableFuture.runAsync(
                        () -> shard.consumeRecords(this.startFrom,
                                recordStopCondition(stopCondition),
                                recordConsumer()), executorService))
                .collect(Collectors.toList());
        // don't chain futureShardPositions with CompletableFuture::join as lazy execution will prevent threads from
        // running in parallel

        stopAllThreads(executorService);
        return StreamPosition.of();
    }

    private BiConsumer<Long, Record> recordConsumer() {
        return (millis, record) -> {
            final Event<String> jsonEvent = createEvent(ofMillis(millis), record);
            registeredConsumers().encodeAndSend(jsonEvent);
        };
    }

    private BiFunction<Long, Record, Boolean> recordStopCondition(final Predicate<Event<?>> stopCondition) {
        return (millis, record) -> {
            if (record == null) {
                return stopCondition.test(Event.event(null, null, null, null, ofMillis(millis)));
            }
            return stopCondition.test(createEvent(ofMillis(millis), record));
        };
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

}
