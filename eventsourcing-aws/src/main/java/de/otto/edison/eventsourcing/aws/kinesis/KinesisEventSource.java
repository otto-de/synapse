package de.otto.edison.eventsourcing.aws.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.AbstractEventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static de.otto.edison.eventsourcing.aws.kinesis.KinesisMessage.kinesisMessage;
import static de.otto.edison.eventsourcing.message.Header.responseHeader;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;

public class KinesisEventSource extends AbstractEventSource {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisEventSource.class);

    private final KinesisStream kinesisStream;

    public KinesisEventSource(final String name,
                              final KinesisStream kinesisStream,
                              final ApplicationEventPublisher eventPublisher,
                              final ObjectMapper objectMapper) {
        super(name, eventPublisher, objectMapper);
        this.kinesisStream = kinesisStream;
    }

    @Override
    public String getStreamName() {
        return kinesisStream.getStreamName();
    }

    @Override
    public StreamPosition consumeAll(final StreamPosition startFrom,
                                     final Predicate<Message<?>> stopCondition) {
        publishEvent(startFrom, EventSourceNotification.Status.STARTED, "Consuming messages from Kinesis.");

        try {
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

                StreamPosition streamPosition = StreamPosition.of(shardPositions);
                publishEvent(streamPosition, EventSourceNotification.Status.FINISHED, "Stopped consuming messages from Kinesis.");
                return streamPosition;
            } finally {
                stopAllThreads(executorService);
            }

        } catch (Exception e) {
            publishEvent(StreamPosition.of(), EventSourceNotification.Status.FAILED, "Error consuming messages from Kinesis: " + e.getMessage());
            throw e;
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

    private Message<String> createEvent(Duration durationBehind, Record record) {
        return kinesisMessage(durationBehind, record, byteBuffer -> {
            if (byteBuffer.equals(ByteBuffer.allocateDirect(0))) {
                return null;
            } else {
                return UTF_8.decode(record.data()).toString();
            }
        });
    }

    private BiFunction<Long, Record, Boolean> recordStopCondition(final Predicate<Message<?>> stopCondition) {
        return (millis, record) -> {
            if (record == null) {
                return stopCondition.test(Message.message(
                        "",
                        responseHeader(null, null, ofMillis(millis)),
                        null
                ));
            }
            return stopCondition.test(createEvent(ofMillis(millis), record));
        };
    }

    private BiConsumer<Long, Record> recordConsumer() {
        return (millis, record) -> {
            final Message<String> jsonMessage = createEvent(ofMillis(millis), record);
            registeredConsumers().encodeAndSend(jsonMessage);
        };
    }
}
